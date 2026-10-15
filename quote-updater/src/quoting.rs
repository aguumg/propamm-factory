//! Builder mode: quote every configured builder with the same signed update.
//!
//! The main loop owns everything decided once per block — the target block, its stamp,
//! the pending nonce, and signing the transaction — and publishes the result as an
//! immutable `Arc<Quote>`. One task per builder reads that channel and owns only wire
//! state: its connection, uuid, sequence number, redial timer and requote ticker. The
//! chain head it rolls over on comes from a channel too, fed by one process-wide watcher
//! (head.rs), so no RPC round-trip sits between a price move and its signature.
//!
//! The split is not stylistic. A builder's socket is where its backpressure lives: a
//! write blocks when its buffers are full, and its responses — read between sends and
//! matched to the update they answer — are what bound how much it is sent (MAX_UNACKED)
//! and how long it may stay silent (ACK_TIMEOUT, 5s), while builders evict a quote left
//! idle past ~400ms. In one shared loop a builder that is TCP-alive but slow would cost
//! *every* builder its quote; per-task, it costs only its own.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use ethrex_common::{Address, U256};
use ethrex_rpc::{
    clients::eth::EthClient,
    types::block_identifier::{BlockIdentifier, BlockTag},
};
use eyre::{Result, ensure, eyre};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use url::Url;

use crate::{
    Landing, LandingTracker, Live, RUNWAY_CHECK_BLOCKS, SendOpts, bounded,
    builder::{Ack, BuilderClient, QuoteUpdate},
    config::{BuilderConfig, BuildersConfig},
    guard::{Latch, TripReason},
    head::{self, Head},
    landing_label,
    metrics::{BuilderMetrics, Metrics, PairMetrics, PublishAction, RpcCall, timed},
    runway_check,
    supervisor::Shutdown,
    update::{UpdateParams, UpdateStamp, sign_update_tx, wall_clock_secs},
    verify_landed,
};

/// Delay between redial attempts for a disconnected builder. Matches binance.rs: without
/// it a dead endpoint is redialed on every requote tick, 20 times a second by default.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How long a halted pair waits for its builders to report the cancel before printing the
/// block's last summary. Only the log line depends on it — `run`'s wind-down is what
/// actually guarantees the withdraw — so it is short enough not to delay the halt.
const HALT_REPORT_GRACE: Duration = Duration::from_millis(500);

/// How long an update may go unanswered before its connection is judged dead and
/// redialed, whatever the socket says. Measured on the oldest unanswered update.
pub const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a shutdown waits for the builder tasks to withdraw their quotes and exit.
/// ACK_TIMEOUT plus a second of slack, so a withdraw that is merely slow still completes
/// while a hung builder cannot wedge the exit.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(6);

/// Depth of the builder-event channel. Tasks only report transitions, so this is deep
/// enough that `try_send` never drops in practice.
pub const EVENT_CHANNEL_CAPACITY: usize = 256;

/// How many updates one builder may hold unanswered before the task stops sending it
/// more until it answers (or ACK_TIMEOUT drops the connection). Sends do not wait for
/// their acks, so this is what bounds the flood at a builder that accepts every write and
/// answers none: at the default 50ms cadence the cap is reached in 400ms, well inside
/// ACK_TIMEOUT. Sized so a builder that is merely slow (a few hundred ms per ack) is never
/// throttled by it.
pub const MAX_UNACKED: usize = 8;

/// One block's signed update, shared by every builder task.
///
/// Signed once, centrally, and never per builder: every builder receives byte-identical
/// transaction bytes, so the same nonce and the same hash mean at most one inclusion
/// however many builders hold the quote. Signing per builder (each with its own nonce)
/// would let two builders both win and land two updateState calls in one block.
pub struct Quote {
    /// Raw signed transaction (type byte + RLP). Empty withdraws the quote.
    pub raw_tx: Vec<u8>,
    /// The single block this quote is valid for.
    pub block_number: u64,
}

/// What every builder should be doing right now.
#[derive(Clone)]
pub enum Command {
    /// Nothing to quote: startup, or the target block just passed. Its quote died with
    /// the block, so a cancel would be wasted traffic — tasks simply go idle.
    Idle,
    /// Quote this for its block, requoting until superseded.
    Quote(Arc<Quote>),
    /// Actively withdraw any live quote for this block. The block is still current and
    /// the builder is holding a quote nobody vouches for any more, which only an
    /// explicit empty-tx update retracts.
    Withdraw { block_number: u64 },
}

/// The parts of a quote update that are the same for every block and every builder.
#[derive(Clone, Copy)]
pub struct QuoteIdentity {
    /// token0 ++ token1, the pair whose prices these updates influence.
    pub asset_pairs: [[u8; 40]; 1],
    /// The contract takers quote against.
    pub quote_address: [u8; 20],
    /// The pools these updates apply to.
    pub pool_addresses: [[u8; 20]; 1],
}

impl QuoteIdentity {
    pub fn new((token0, token1): (Address, Address), target: Address) -> Self {
        let mut asset_pair = [0u8; 40];
        asset_pair[..20].copy_from_slice(token0.as_bytes());
        asset_pair[20..].copy_from_slice(token1.as_bytes());
        let quote_address = target.to_fixed_bytes();
        Self {
            asset_pairs: [asset_pair],
            quote_address,
            pool_addresses: [quote_address],
        }
    }
}

/// Something worth saying about one builder. Tasks report only transitions, so the main
/// loop can print one aggregated line per block instead of a line per requote.
#[derive(Debug)]
pub enum EventKind {
    /// First successful ack for a block.
    Acked { latency_ms: u128 },
    /// A live quote was withdrawn.
    Withdrawn,
    /// The builder answered with a non-empty error.
    Rejected { error: String },
    /// The connection failed or was lost.
    Disconnected { error: String },
    /// A redial succeeded.
    Reconnected,
}

#[derive(Debug)]
pub struct Event {
    pub name: String,
    /// The block this concerns, or None for connection-level events.
    pub block_number: Option<u64>,
    pub kind: EventKind,
}

/// One builder's connection and the wire state that belongs to it alone.
pub struct BuilderTask {
    config: BuilderConfig,
    identity: QuoteIdentity,
    disable_cross_region: bool,
    requote: Duration,
    conn: Option<BuilderClient>,
    /// This builder's pre-bound metric handles, one per `(builder, pair)`. Recorded at
    /// the source in `send` and `ensure_connected`, never derived from the `Event`
    /// channel below: `report` is a drop-on-full `try_send`, so a channel-derived counter
    /// would undercount exactly when the builder is struggling hardest.
    metrics: BuilderMetrics,
    /// One uuid per builder, not one shared across them. `replacement_uuid` means
    /// "supersede the previous update with this uuid", and builders sharing
    /// infrastructure could land two of our connections in one quote table, where
    /// independent sequence counters make the lower one lose every race.
    uuid: [u8; 16],
    /// Strictly increasing for this builder, and never reset — not even across a redial.
    /// A counter restarted at 0 while the builder still remembers our old quote under
    /// this uuid has every update silently ignored as stale until it climbs back past the
    /// old high-water mark. That is why it lives here, outside the connection's lifetime.
    seq: u64,
    /// The block of the last non-empty quote written to this builder, cleared by writing
    /// a withdraw. While set, the builder may hold a live quote of ours, which is what
    /// drives withdraw-on-stale and withdraw-on-shutdown. Taken from the *write*, not the
    /// ack: sends do not wait for their acks (see `run`), so a withdraw commanded while a
    /// quote's ack is still in flight must go out, and it would be lost if this waited
    /// for the ack to say the quote was live.
    live_block: Option<u64>,
    /// Updates written and not yet answered, oldest first: what a response is matched
    /// against, and what ACK_TIMEOUT is measured on. Never longer than MAX_UNACKED.
    unacked: VecDeque<Unacked>,
    /// Whether a send was skipped for the cap, so the next response that frees a slot
    /// sends what that tick would have.
    starved: bool,
    /// When this builder was last dialed, so a dead endpoint is not redialed on every
    /// tick. None means never dialed: connect immediately.
    last_attempt: Option<std::time::Instant>,
    /// Whether this task has ever held a live connection, which is what separates a
    /// successful first connect from a successful redial. Inferring that from
    /// `last_attempt` gets the normal case wrong: a builder that connected at startup is
    /// handed its client, so it has a connection it never dialed itself, and its first
    /// redial after a drop would report `Disconnected` and never `Reconnected` — an
    /// operator would watch it go down and never see it come back.
    ever_connected: bool,
    /// The last error reported, so a builder that is down does not emit a line every
    /// RECONNECT_DELAY forever.
    last_error: Option<String>,
    /// The last block this builder acked for, so only the first ack per block is
    /// reported: the per-task equivalent of the old `sent == 1` check.
    acked_block: Option<u64>,
    /// The last block this builder was rejected for, so only the first rejection per
    /// block is reported. Unlike `Acked` and `Disconnected`, a rejection was previously
    /// reported on every send — up to one per requote tick — and the event channel is a
    /// shared bounded `mpsc` with a drop-on-full `try_send`, so a persistently-rejecting
    /// builder (e.g. quoting a pool or pair it does not recognize) could saturate it and
    /// drop other builders' events, including a `Reconnected` that `BlockSummary` needs
    /// to clear a `Down` column — and a dropped `Reconnected` is permanent.
    rejected_block: Option<u64>,
}

/// Why a builder connection is being forgotten. Only a wire that broke is a
/// `send_errors` increment; one closed on purpose is not, and the two used to be
/// indistinguishable because every path funnelled through `drop_connection`.
///
/// The distinction is not cosmetic: `wind_down` sends the shutdown withdraw and then waits
/// ACK_TIMEOUT for its ack, so a builder merely slow to answer a *deliberate* close booked a
/// transport failure on a healthy socket — a `rate(builder_send_errors_total)` spike at
/// every restart of the service, in the one series an operator reads to decide whether a
/// builder is flaky.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disconnect {
    /// The wire failed: a write that errored, an ack that could not be read, or an ack that
    /// never came while the pair was still quoting. A clean server-side close counts here
    /// too — from this side it is indistinguishable from a crash, and either way the quote
    /// this builder was holding is gone.
    Failed,
    /// We are done with the socket: the shutdown withdraw's ack did not arrive inside the
    /// grace `wind_down` allows it. Best-effort by design (see `wind_down`), so not a
    /// failure to count.
    Deliberate,
}

/// One update on the wire, awaiting its response.
struct Unacked {
    seq: u64,
    block_number: u64,
    /// An empty-tx update: its ack means the builder holds nothing of ours.
    withdraw: bool,
    sent_at: tokio::time::Instant,
}

/// What woke the task's loop.
enum Woke {
    Stop,
    /// A new command, or a requote tick: send what the channel holds.
    Send,
    /// The connection produced a response, or failed trying.
    Ack(Result<Ack>),
    /// The oldest unanswered update has waited ACK_TIMEOUT.
    AckTimeout,
}

impl BuilderTask {
    pub fn new(
        config: BuilderConfig,
        identity: QuoteIdentity,
        disable_cross_region: bool,
        requote: Duration,
        conn: Option<BuilderClient>,
        metrics: BuilderMetrics,
    ) -> Self {
        let disable_cross_region = config.disable_cross_region_or(disable_cross_region);
        // Set here, not left at the `Gauge` default of 0, so a builder handed a live
        // connection at startup is not reported down until it actually drops.
        metrics.up.set(if conn.is_some() { 1.0 } else { 0.0 });
        Self {
            config,
            identity,
            disable_cross_region,
            requote,
            metrics,
            // A connection handed in counts as having been up, but deliberately does not
            // count as an attempt: pre-loading `last_attempt` would also make the first
            // redial after a drop wait out RECONNECT_DELAY for no reason.
            ever_connected: conn.is_some(),
            conn,
            uuid: rand::random(),
            seq: 0,
            live_block: None,
            unacked: VecDeque::new(),
            starved: false,
            last_attempt: None,
            last_error: None,
            acked_block: None,
            rejected_block: None,
        }
    }

    /// Quotes whatever the channel currently holds, at this builder's own cadence, until
    /// told to stop. Never returns an error: one builder's outage must not stop the
    /// others, so everything is reported and retried rather than propagated.
    ///
    /// A write never waits for its ack. Responses are read here, between sends, and
    /// matched to the update they answer through `unacked`, so a price move that lands
    /// while an earlier update is still unanswered goes out at once rather than one ack
    /// round-trip later. What bounds a builder that accepts writes and answers none is
    /// MAX_UNACKED on how many may be outstanding and ACK_TIMEOUT on how long the oldest
    /// may wait.
    pub async fn run(
        mut self,
        mut cmd: watch::Receiver<Command>,
        mut stop: watch::Receiver<bool>,
        events: mpsc::Sender<Event>,
    ) {
        let mut ticker = tokio::time::interval(self.requote);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // The oldest unanswered update's deadline. The sleep below is built fresh on
            // every pass, which loses nothing: the deadline lives here, not in the sleep.
            let ack_deadline = self
                .unacked
                .front()
                .map(|unacked| unacked.sent_at + ACK_TIMEOUT);
            let woke = tokio::select! {
                _ = stop.changed() => Woke::Stop,
                // A price move must not wait for the next tick.
                _ = cmd.changed() => Woke::Send,
                _ = ticker.tick() => Woke::Send,
                ack = read_ack(&mut self.conn) => Woke::Ack(ack),
                // tokio evaluates every arm's expression whether or not the arm is
                // enabled, so this one must not need the deadline to exist.
                _ = tokio::time::sleep_until(
                    ack_deadline.unwrap_or_else(tokio::time::Instant::now)
                ), if ack_deadline.is_some() => Woke::AckTimeout,
            };
            let due = match woke {
                Woke::Stop => {
                    self.wind_down(&events).await;
                    return;
                }
                Woke::Send => true,
                Woke::Ack(ack) => {
                    self.on_ack(ack, &events);
                    std::mem::take(&mut self.starved)
                }
                Woke::AckTimeout => {
                    self.on_ack_timeout(&events, Disconnect::Failed);
                    std::mem::take(&mut self.starved)
                }
            };
            if !due {
                continue;
            }

            // Clone the Arc, not the bytes, and drop the watch guard before awaiting.
            let command = cmd.borrow_and_update().clone();
            let (quote, block_number) = match command {
                Command::Idle => continue,
                Command::Quote(ref quote) => (Some(quote.clone()), quote.block_number),
                Command::Withdraw { .. } if self.live_block.is_none() => continue,
                Command::Withdraw { block_number } => (None, block_number),
            };
            if !self.ensure_connected(&events).await {
                continue;
            }
            let tx = quote
                .as_deref()
                .map_or(&[][..], |quote| quote.raw_tx.as_slice());
            self.send(tx, block_number, &events).await;
        }
    }

    /// Writes one update — an empty `tx` is a withdraw — and records it as awaiting its
    /// response, without waiting for it (see `run`). Skipped, and remembered as skipped,
    /// while MAX_UNACKED updates are outstanding.
    async fn send(&mut self, tx: &[u8], block_number: u64, events: &mpsc::Sender<Event>) {
        if self.unacked.len() >= MAX_UNACKED {
            self.starved = true;
            return;
        }
        self.seq += 1;
        // Copied out so the borrow of self.conn below does not conflict with them.
        let (uuid, seq, identity, disable) = (
            self.uuid,
            self.seq,
            self.identity,
            self.disable_cross_region,
        );
        let Some(conn) = self.conn.as_mut() else {
            return;
        };
        let update = QuoteUpdate {
            tx,
            block_number,
            replacement_uuid: uuid,
            replacement_seq_number: seq,
            disable_cross_region_sharing: disable,
            asset_pairs: &identity.asset_pairs,
            quote_address: identity.quote_address,
            pool_addresses: &identity.pool_addresses,
        };
        match conn.write(&update).await {
            Ok(()) => {
                let withdraw = tx.is_empty();
                self.unacked.push_back(Unacked {
                    seq,
                    block_number,
                    withdraw,
                    sent_at: tokio::time::Instant::now(),
                });
                self.starved = false;
                // From the write, not the ack: see the field.
                self.live_block = if withdraw { None } else { Some(block_number) };
            }
            Err(err) => {
                // The socket is unusable, so drop it and let the next tick redial.
                // `live_block` is deliberately left as it was: an evicted quote clears
                // itself, and assuming it cleared would suppress a withdraw that still
                // needs sending if the connection comes back.
                self.drop_connection(Disconnect::Failed);
                self.report_once(events, Some(block_number), format!("{err:#}"));
            }
        }
    }

    /// Folds in one response, or the failure that came instead of one.
    fn on_ack(&mut self, ack: Result<Ack>, events: &mpsc::Sender<Event>) {
        let ack = match ack {
            Ok(ack) => ack,
            Err(err) => {
                let block = self.unacked.front().map(|unacked| unacked.block_number);
                self.drop_connection(Disconnect::Failed);
                self.report_once(events, block, format!("{err:#}"));
                return;
            }
        };
        if ack.replacement_uuid != self.uuid {
            return;
        }
        let Some(answered) = self.answered(ack.replacement_seq_number) else {
            return;
        };
        let block_number = answered.block_number;
        if ack.error.is_empty() {
            self.last_error = None;
            // The acked seq, not the sent one: recorded here rather than next to
            // `self.seq += 1` in `send` so a frozen gauge means acks stopped arriving
            // rather than that sending stopped. See the HELP string in metrics.rs.
            self.metrics.seq.set(answered.seq as f64);
            if answered.withdraw {
                self.metrics.withdrawals.inc();
                // A withdrawn quote is no longer this block's acked quote. Leaving it
                // set would silence the next quote for the *same* still-current
                // block — the common case on a flaky feed, where the price goes
                // unusable and then recovers — so the event stream would read
                // `withdrawn` and then nothing while the builder actively quotes.
                self.acked_block = None;
                self.report(events, Some(block_number), EventKind::Withdrawn);
            } else {
                let latency = answered.sent_at.elapsed();
                // Counted here, outside the `acked_block` throttle below, rather than
                // inside it: that throttle exists to keep the per-block operator summary
                // readable (one line per block, however many requotes land in it), and a
                // counter that inherited it would report one ack per block for a builder
                // acking twenty times a second. Seeing every ack — and every withdrawal —
                // is the entire point of these two counters.
                self.metrics.acks.inc();
                // From the write, not from this response arriving: `sent_at` is stamped in
                // `send`, which no longer waits for the ack, so this is still the
                // send-to-ack time the HELP string claims.
                self.metrics.ack_latency.observe(latency.as_secs_f64());
                if self.acked_block != Some(block_number) {
                    self.acked_block = Some(block_number);
                    self.report(
                        events,
                        Some(block_number),
                        EventKind::Acked {
                            latency_ms: latency.as_millis(),
                        },
                    );
                }
            }
        } else {
            // Unconditional, unlike the report below: a builder rejecting twenty times a
            // second must count as twenty, not one, so the throttle sits inside this arm
            // rather than gating which responses are counted at all.
            self.metrics.rejections.inc();
            // Throttled the same way as `Acked` above, and for the same shared-channel
            // reason: only the first rejection per block is reported, so a builder
            // rejecting every requote of a misconfigured pair costs the event channel one
            // slot per block rather than one per tick.
            if self.rejected_block != Some(block_number) {
                self.rejected_block = Some(block_number);
                self.report(
                    events,
                    Some(block_number),
                    EventKind::Rejected { error: ack.error },
                );
            }
        }
    }

    /// The unanswered update that `seq` answers, dropping any older ones: a builder that
    /// answers only the latest update it holds has superseded those, and their
    /// responses will never come.
    fn answered(&mut self, seq: u64) -> Option<Unacked> {
        while self
            .unacked
            .front()
            .is_some_and(|unacked| unacked.seq < seq)
        {
            self.unacked.pop_front();
        }
        if self
            .unacked
            .front()
            .is_some_and(|unacked| unacked.seq == seq)
        {
            self.unacked.pop_front()
        } else {
            None
        }
    }

    /// The oldest unanswered update has waited ACK_TIMEOUT: the connection is judged
    /// dead, whatever the socket says.
    ///
    /// `cause` because both callers land here for different reasons: the quote loop, where a
    /// silent builder is a wire failure, and `wind_down`, where the same silence is a
    /// shutdown we are choosing not to wait out any longer.
    fn on_ack_timeout(&mut self, events: &mpsc::Sender<Event>, cause: Disconnect) {
        let block = self.unacked.front().map(|unacked| unacked.block_number);
        self.drop_connection(cause);
        self.report_once(events, block, format!("no response in {ACK_TIMEOUT:?}"));
    }

    /// Forgets the connection and everything in flight on it. `live_block` stays: see
    /// `send`.
    ///
    /// Also where the two wire metrics are recorded, because this is the one place that
    /// decides a connection is gone: `send`'s failed write, a failed ack read, and both
    /// ACK_TIMEOUT paths all arrive here. Counting at the call sites instead would have
    /// missed a path — the live-loop timeout did not exist when these counters were added —
    /// which is why `cause` is a parameter rather than the caller's business: every route in
    /// has to state which kind of ending it is, and `Disconnect` is where the two are
    /// described.
    ///
    /// `up` goes to 0 either way, because either way there is no connection.
    fn drop_connection(&mut self, cause: Disconnect) {
        if cause == Disconnect::Failed {
            self.metrics.send_errors.inc();
        }
        self.metrics.up.set(0.0);
        self.conn = None;
        self.unacked.clear();
        self.starved = false;
    }

    /// Ensures a live connection, redialing at most once per RECONNECT_DELAY. Returns
    /// false when this tick has nothing to send through.
    async fn ensure_connected(&mut self, events: &mpsc::Sender<Event>) -> bool {
        if self.conn.is_some() {
            return true;
        }
        if self
            .last_attempt
            .is_some_and(|at| at.elapsed() < RECONNECT_DELAY)
        {
            return false;
        }
        // Ever *connected*, not ever dialed: see the field's comment.
        let reconnect = self.ever_connected;
        self.last_attempt = Some(std::time::Instant::now());
        match BuilderClient::connect(&self.config.endpoint, &self.config.api_key).await {
            Ok(conn) => {
                self.conn = Some(conn);
                self.ever_connected = true;
                self.metrics.up.set(1.0);
                if reconnect {
                    self.metrics.reconnects.inc();
                }
                self.last_error = None;
                if reconnect {
                    self.report(events, None, EventKind::Reconnected);
                }
                true
            }
            Err(err) => {
                // Redundant today, not dead: every path that reaches this arm has already
                // zeroed `up` — either `new` (handed no connection) or `drop_connection`
                // (which clears `self.conn` and zeroes `up` itself) — so this write changes
                // nothing observable right now. Kept anyway, rather
                // than deleted, so the gauge's correctness is a local invariant of this
                // arm and does not depend on every remote call site that can null out
                // `self.conn` remembering to zero `up` on its own behalf.
                self.metrics.up.set(0.0);
                self.report_once(events, None, format!("{err:#}"));
                false
            }
        }
    }

    /// Withdraws this builder's live quote before the task exits, so a shutdown does not
    /// leave a quote standing that nothing is refreshing any more, and waits for the
    /// withdraw's ack, bounded by ACK_TIMEOUT, which SHUTDOWN_GRACE is sized to cover.
    ///
    /// Best-effort, not a guarantee: a builder that never answers keeps the quote until
    /// it evicts it, and a task whose connection has already been dropped has nothing to
    /// withdraw through — but a dropped socket is itself an effective retraction. So
    /// "shutdown retracts live quotes" holds most of the time, not always.
    async fn wind_down(&mut self, events: &mpsc::Sender<Event>) {
        let Some(block_number) = self.live_block else {
            return;
        };
        if self.conn.is_none() {
            return;
        }
        // Whatever is still unanswered is superseded by the withdraw; clearing it also
        // lifts the cap for it.
        self.unacked.clear();
        self.send(&[], block_number, events).await;
        let deadline = tokio::time::Instant::now() + ACK_TIMEOUT;
        while self.conn.is_some() && !self.unacked.is_empty() {
            match tokio::time::timeout_at(deadline, read_ack(&mut self.conn)).await {
                Ok(ack) => self.on_ack(ack, events),
                // Deliberate: we asked to close, and the withdraw's ack is best-effort.
                Err(_) => self.on_ack_timeout(events, Disconnect::Deliberate),
            }
        }
    }

    /// Reports a failure, but only when the error text changed. An endpoint that is down
    /// otherwise emits a line every RECONNECT_DELAY forever, drowning the per-block lines
    /// that matter.
    fn report_once(&mut self, events: &mpsc::Sender<Event>, block: Option<u64>, error: String) {
        if self.last_error.as_deref() == Some(error.as_str()) {
            return;
        }
        self.last_error = Some(error.clone());
        self.report(events, block, EventKind::Disconnected { error });
    }

    /// `try_send`, never `send`: a full channel must drop a log line rather than block a
    /// quoting task until the main loop drains it.
    fn report(&self, events: &mpsc::Sender<Event>, block_number: Option<u64>, kind: EventKind) {
        let _ = events.try_send(Event {
            name: self.config.name.clone(),
            block_number,
            kind,
        });
    }
}

/// The next response on `conn`, or never while there is none. Keeps `run`'s select! arm
/// well-formed while disconnected: tokio evaluates every arm's expression whether or not
/// the arm is enabled, so the expression itself must cope with having no connection.
async fn read_ack(conn: &mut Option<BuilderClient>) -> Result<Ack> {
    match conn {
        Some(conn) => conn.read_ack().await,
        None => std::future::pending().await,
    }
}

/// Dials every configured builder at once, so startup costs one CONNECT_TIMEOUT rather
/// than one per builder.
pub async fn connect_all(builders: &[BuilderConfig]) -> Vec<Result<BuilderClient>> {
    futures_util::future::join_all(
        builders
            .iter()
            .map(|b| BuilderClient::connect(&b.endpoint, &b.api_key)),
    )
    .await
}

/// The startup table. Takes the outcome per builder as an optional error string so it is
/// testable without opening a socket. Prints the endpoint's host but never the API key.
fn startup_table(path: &str, builders: &[BuilderConfig], errors: &[Option<String>]) -> String {
    let width = builders.iter().map(|b| b.name.len()).max().unwrap_or(0);
    let mut out = format!("builders: {} configured from {path}\n", builders.len());
    let mut live = 0;
    for (builder, error) in builders.iter().zip(errors) {
        match error {
            None => {
                live += 1;
                let host = Url::parse(&builder.endpoint)
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_owned))
                    .unwrap_or_else(|| "?".to_owned());
                out += &format!("  {:width$}  connected  ({host})\n", builder.name);
            }
            Some(error) => {
                out += &format!(
                    "  {:width$}  FAILED     {error}; will retry\n",
                    builder.name
                );
            }
        }
    }
    out += &format!("quoting with {live}/{} builders live", builders.len());
    out
}

/// Refuses to start when nothing connected. A whole config that fails is a typo or a
/// revoked key, which should be loud; one builder failing is an outage, which must not
/// stop the others.
fn ensure_any_connected(path: &str, total: usize, live: usize) -> Result<()> {
    ensure!(
        live > 0,
        "no builder connected ({total} configured); check the endpoints and API keys in \
         {path}, or pass --mode node to send updateState transactions to --rpc-url instead"
    );
    Ok(())
}

/// Waits for every builder task to finish winding down, bounded by SHUTDOWN_GRACE, and
/// returns the names of those that did not. The bound is what stops one hung builder from
/// wedging the exit.
pub async fn join_tasks(tasks: Vec<(String, JoinHandle<()>)>) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
    let mut stragglers = Vec::new();
    for (name, task) in tasks {
        if tokio::time::timeout_at(deadline, task).await.is_err() {
            stragglers.push(name);
        }
    }
    stragglers
}

/// What a block was quoting, for its log line.
pub enum Quoted {
    Slots {
        delta: U256,
        mid: U256,
        ts: u32,
    },
    /// No usable price for this block, and why. A block that prints nothing would be
    /// indistinguishable from a stalled service.
    Withdrawn(String),
}

/// How one builder answered during a block.
enum Outcome {
    /// Configured, connected, but nothing recorded yet this block.
    Pending,
    Acked {
        latency_ms: u128,
    },
    Withdrawn,
    Rejected(String),
    /// Disconnected, and why. Survives across blocks until a reconnect clears it. The
    /// reason is carried rather than dropped because this line is the only thing that ever
    /// prints it: a task reports a lost connection once per distinct error and nothing else
    /// consumes that report, so "down" alone would leave an operator with no idea whether
    /// the endpoint refused the key or the socket died.
    Down(String),
}

/// Accumulates one block's builder events into the single line it prints. Separate from
/// the loop so the output an operator actually reads is testable without a chain or a
/// builder.
pub struct BlockSummary {
    block_number: u64,
    /// In configuration order, so the columns never shuffle between blocks: a glance down
    /// one column always means the same builder.
    outcomes: Vec<(String, Outcome)>,
}

impl BlockSummary {
    pub fn new(block_number: u64, names: &[String]) -> Self {
        Self {
            block_number,
            outcomes: names
                .iter()
                .map(|name| (name.clone(), Outcome::Pending))
                .collect(),
        }
    }

    /// Folds one event in. Connection-level events apply whatever block they name, since
    /// being down outlives the block it was noticed in; the rest answer for one specific
    /// block and are ignored when they name another.
    pub fn record(&mut self, event: &Event) {
        let Some((_, outcome)) = self
            .outcomes
            .iter_mut()
            .find(|(name, _)| *name == event.name)
        else {
            return; // not a configured builder; never happens, must not panic
        };
        *outcome = match &event.kind {
            // Connection state, so it applies regardless of the block it was noticed in: a
            // send that failed for the block that just passed still means the builder is
            // down now, and dropping it as stale would show that column as merely silent.
            EventKind::Disconnected { error } => Outcome::Down(error.clone()),
            EventKind::Reconnected => Outcome::Pending,
            // A per-block answer for a block that is no longer current: a task's ack can
            // land after the loop has rolled to the next target, and folding it in would
            // show that column as having answered for a block it has not been asked about
            // yet.
            _ if event.block_number != Some(self.block_number) => return,
            EventKind::Acked { latency_ms } => Outcome::Acked {
                latency_ms: *latency_ms,
            },
            EventKind::Withdrawn => Outcome::Withdrawn,
            EventKind::Rejected { error } => Outcome::Rejected(error.clone()),
        };
    }

    /// How many builders are not known to be down.
    pub fn live(&self) -> usize {
        self.outcomes
            .iter()
            .filter(|(_, outcome)| !matches!(outcome, Outcome::Down(_)))
            .count()
    }

    /// Whether any builder's latest answer for this block is an ack of a non-empty
    /// transaction — the condition under which a landing was expected at all, so the
    /// post-block read-back has something to verify. A builder whose quote was withdrawn
    /// afterwards reads `Withdrawn` and does not count.
    pub fn any_acked(&self) -> bool {
        self.outcomes
            .iter()
            .any(|(_, outcome)| matches!(outcome, Outcome::Acked { .. }))
    }

    /// Whether any builder might still report a cancel for this block: one that is down
    /// cannot, one that has reported withdrawn need not, and every other — acked this
    /// block, or pending because `carry_over` reset a builder still active from the block
    /// before — might. What the halt's bounded wait is for.
    pub fn awaiting_cancels(&self) -> bool {
        self.outcomes
            .iter()
            .any(|(_, outcome)| !matches!(outcome, Outcome::Down(_) | Outcome::Withdrawn))
    }

    /// Folds one event in, same as `record`, and additionally reports the standalone line
    /// a reconnect prints between blocks. Split out from `record` rather than printing
    /// inline in the loop because `drive` folds events at two call sites — a `select!` arm
    /// while the block is live, and a post-loop drain for whatever arrived right as it
    /// broke out — and each event flows through the channel exactly once, so whichever site
    /// actually receives a given `Reconnected` event is the one that must print for it.
    /// Returning the line (rather than printing here) keeps this testable without stdout
    /// capture.
    ///
    /// A disconnect gets no matching line: it is already legible as the very next block's
    /// column reading "down", where a reconnect is not — the column just goes back to
    /// quoting silently, which reads as nothing having happened.
    pub fn fold(&mut self, event: &Event) -> Option<String> {
        self.record(event);
        matches!(event.kind, EventKind::Reconnected).then(|| {
            format!(
                "builder {} reconnected; {}/{} live",
                event.name,
                self.live(),
                self.outcomes.len()
            )
        })
    }

    /// Carries this block's per-builder state into the next block's summary: a builder
    /// that is down stays down until it reconnects, so the column does not flicker back
    /// to "quoting" every block.
    pub fn carry_over(&self, block_number: u64) -> Self {
        Self {
            block_number,
            outcomes: self
                .outcomes
                .iter()
                .map(|(name, outcome)| {
                    let carried = match outcome {
                        Outcome::Down(error) => Outcome::Down(error.clone()),
                        _ => Outcome::Pending,
                    };
                    (name.clone(), carried)
                })
                .collect(),
        }
    }

    /// The two lines a block prints: what was quoted, then how each builder answered.
    pub fn render(&self, quoted: &Quoted) -> String {
        let header = match quoted {
            Quoted::Slots { delta, mid, ts } => format!(
                "quoting block {}: slots = [{delta}, {mid}] at timestamp {ts}",
                self.block_number
            ),
            Quoted::Withdrawn(reason) => {
                format!("quoting block {}: withdrawn ({reason})", self.block_number)
            }
        };
        let columns: Vec<String> = self
            .outcomes
            .iter()
            .map(|(name, outcome)| match outcome {
                Outcome::Pending => format!("{name} no ack"),
                Outcome::Acked { latency_ms } => format!("{name} ack {latency_ms}ms"),
                Outcome::Withdrawn => format!("{name} withdrawn"),
                Outcome::Rejected(error) => format!("{name} rejected: {error}"),
                Outcome::Down(error) => format!("{name} down: {error}"),
            })
            .collect();
        format!("{header}\n  {}", columns.join(" | "))
    }
}

/// Connects every builder, spawns a task each, and drives them for one pair until
/// shutdown or (with --once) the first target block passes.
///
/// One call per pair, running under its own `supervisor::supervise`: each pair signs with
/// its own key and quotes its own lane, so it gets its own connections, its own uuids and
/// its own per-block summary, every line prefixed with the pair's label. Everything is
/// taken owned (and the caller's closure clones per restart) because a supervised restart
/// re-enters here from scratch — reconnecting the builders is exactly what a restart is
/// for.
// Eight, one past clippy's threshold, for the same reason `drive` below is allowed nine:
// every one of them is a distinct thing the caller owns — two channels, the pair, the
// builder config, the options, the registry — and a struct bundling them would carry the
// identical field list to exactly one call site while adding a name for the bundle.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: EthClient,
    live: Live,
    opts: SendOpts,
    config: Arc<BuildersConfig>,
    builders_path: String,
    mut head: watch::Receiver<Head>,
    cancel: Shutdown,
    metrics: Arc<Metrics>,
) -> Result<Ended> {
    let label = live.pair.label.clone();
    let outcomes = connect_all(&config.builders).await;
    let errors: Vec<Option<String>> = outcomes
        .iter()
        .map(|outcome| outcome.as_ref().err().map(|err| format!("{err:#}")))
        .collect();
    for line in startup_table(&builders_path, &config.builders, &errors).lines() {
        tracing::info!("[{label}] {line}");
    }
    let connected = errors.iter().filter(|error| error.is_none()).count();
    ensure_any_connected(&builders_path, config.builders.len(), connected)?;

    let (cmd_tx, cmd_rx) = watch::channel(Command::Idle);
    let (stop_tx, stop_rx) = watch::channel(false);
    let (event_tx, mut events) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let identity = QuoteIdentity::new(live.pair.tokens, opts.target);
    // The four fields the signing path (update_calldata, sign_update_tx) needs, narrowed
    // so it does not have to be threaded the whole SendOpts.
    let params = UpdateParams {
        registry: opts.registry,
        target: opts.target,
        lane: live.pair.lane,
        chain_id: opts.chain_id,
    };
    let requote = Duration::from_millis(opts.requote_ms.max(1));
    let names: Vec<String> = config.builders.iter().map(|b| b.name.clone()).collect();

    // A builder that failed to connect is not dropped for the process lifetime; it starts
    // life disconnected and its task redials it.
    let tasks: Vec<(String, JoinHandle<()>)> = config
        .builders
        .iter()
        .zip(outcomes)
        .map(|(builder, outcome)| {
            let builder_metrics = metrics.for_builder(&live.pair.label, &builder.name);
            let task = BuilderTask::new(
                BuilderConfig {
                    name: builder.name.clone(),
                    endpoint: builder.endpoint.clone(),
                    api_key: builder.api_key.clone(),
                    disable_cross_region: builder.disable_cross_region,
                },
                identity,
                opts.disable_cross_region,
                requote,
                outcome.ok(),
                builder_metrics,
            );
            (
                builder.name.clone(),
                tokio::spawn(task.run(cmd_rx.clone(), stop_rx.clone(), event_tx.clone())),
            )
        })
        .collect();

    let result = drive(
        &client,
        &live,
        &params,
        &cmd_tx,
        &mut events,
        &names,
        &opts,
        &mut head,
        cancel,
    )
    .await;

    // Wind down on every exit path, error included: a quote left standing that nothing is
    // refreshing is worse than whatever stopped us.
    let _ = stop_tx.send(true);
    let stragglers = join_tasks(tasks).await;
    if !stragglers.is_empty() {
        tracing::warn!(
            "[{label}] did not finish winding down within {SHUTDOWN_GRACE:?}, so their \
             quotes may still stand until the builder evicts them: {}",
            stragglers.join(", ")
        );
    }
    result
}

/// The per-tick publish decision, pulled out of drive's select! loop (the way
/// BlockSummary was) so the interplay of the dedup cache, the withdraw-once episode
/// latch and the circuit breaker is testable without a chain or a builder.
///
/// One per pair, built from that pair's own `max_deviation`: the state is per lane, so a
/// feed that goes wrong withdraws its own quote and leaves every other pair quoting.
pub struct PublishGate {
    /// The (delta, mid) already signed and published for this block; publishing is
    /// skipped while the price stays there. Reset every block (each needs its own
    /// signature) and on withdraw (the quote is gone at the builders, so "unchanged"
    /// must not swallow the resumption).
    quoted: Option<(U256, U256)>,
    /// Why this block is withdrawn, if it is. Set once per episode — the builders'
    /// own tickers keep the retraction in force — and the first reason wins the
    /// block header.
    withdrawn: Option<String>,
    /// Whether the trip alarm has already been printed. The `Halt` action is deliberately
    /// repeatable — a caller that keeps polling a tripped gate must keep being told to
    /// stop — but the alarm describes a transition, and re-emitting it on every feed move
    /// would flood stderr sub-second on a liquid pair.
    announced_halt: bool,
    /// The lane's latch. The composite task's guards trip it, a panic trips it, a
    /// component's own task can trip it; this gate only reads it. Shared rather than owned
    /// so that a supervised restart, which builds a new gate, finds a trip that happened
    /// while no gate existed waiting for it: the guard has to outlive the loop it guards.
    latch: Latch,
}

/// What drive should do with the command channel this tick, plus a state-transition
/// line to print if the tick crossed one.
pub struct Decision {
    pub action: Action,
    pub announce: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum Action {
    /// Sign and send Command::Quote, then report back via `published` on success.
    Publish((U256, U256)),
    /// Send Command::Withdraw for the current target block: an episode just began.
    Withdraw,
    /// Withdraw and stop this pair for good: the lane's latch tripped. Distinct from
    /// `Withdraw` because it ends the loop rather than waiting for the price to recover:
    /// nothing this process observes can lift it.
    Halt,
    /// Nothing new to send.
    Nothing,
}

/// The metric label for a gate decision. A free function with its own test rather than an
/// inline match arm at the recording site, so a new `Action` variant fails to compile here
/// instead of quietly landing in an existing bucket and making a dashboard lie.
fn action_label(action: &Action) -> PublishAction {
    match action {
        Action::Publish(_) => PublishAction::Publish,
        Action::Withdraw => PublishAction::Withdraw,
        Action::Halt => PublishAction::Halt,
        Action::Nothing => PublishAction::Nothing,
    }
}

/// Why a pair's quote loop ended, which decides whether it should ever run again.
///
/// `supervise` restarts on `Err` and stops on `Ok`, so both variants stop the pair — but
/// they are not the same event, and only one of them means the pusher is now quoting less
/// than it was configured to. Without the distinction a `--once` run that completed
/// normally and a lane halted by its breaker look identical from the outside.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "a Halted outcome has to reach Health::halt, or the watchdog will restart the pair"]
pub enum Ended {
    /// Shutdown, or `--once` completing. Nothing is wrong.
    Finished,
    /// The lane's latch tripped. This pair will not quote again in this process.
    Halted,
}

impl PublishGate {
    pub fn new(latch: Latch) -> Self {
        Self {
            quoted: None,
            withdrawn: None,
            announced_halt: false,
            latch,
        }
    }

    /// Forgets the per-block state: the new block needs its own signature, and its
    /// withdraw episode (if any) is its own.
    ///
    /// The latch deliberately survives this. It holds for the life of the pair, and a
    /// block boundary is not new information about whether the feed can be trusted.
    pub fn begin_block(&mut self) {
        self.quoted = None;
        self.withdrawn = None;
    }

    /// Forgets only the dedup cache, forcing the next usable price to re-sign: a missed
    /// slot moved the stamp on, so the standing quote's signature is for a timestamp no
    /// block can carry any more. A withdraw episode in progress stays withdrawn — there
    /// is nothing to re-stamp until the price is usable.
    pub fn resign(&mut self) {
        self.quoted = None;
    }

    /// The per-tick decision. The latch is read first: the composite task judges every
    /// sample as it arrives, so a price reaching here has already been assessed, and a
    /// second assessment would race the first for the reference. A tripped latch outranks
    /// everything: a usable price, which must halt rather than publish, and an unusable
    /// one, where the stale-feed path must answer with the halt rather than an ordinary
    /// withdraw that `drive` would treat as "wait for the price to come back". `drive`
    /// returns on the first Halt today, but that is a property of one caller, not of the
    /// guard.
    pub fn decide(&mut self, price: Result<(U256, U256)>) -> Decision {
        if let Some(trip) = self.latch.tripped() {
            let reason = trip.reason.clone();
            return self.halt(&reason);
        }
        match price {
            Ok(quote) => self.publish(quote),
            Err(err) => self.withdraw(|| format!("{err:#}")),
        }
    }

    /// The end of the pair, from either path that can reach it. The reason was rendered by
    /// whatever tripped the latch: the three strings here are the deviation guard's exact
    /// wording for a deviation trip, and a registered guard's or a panic's for theirs.
    fn halt(&mut self, reason: &TripReason) -> Decision {
        // Unlike an ordinary episode, a trip's reason wins the header even when an outage
        // already started one: it is the more specific truth, and it is the last thing
        // this pair will ever report.
        self.retract(reason.header.clone());
        // Always Halt, never throttled down to Nothing the way an ordinary withdraw is:
        // the throttle exists to avoid re-sending a standing retraction, and this is not a
        // re-send but the end of the pair.
        let announce = (!self.announced_halt).then(|| {
            self.announced_halt = true;
            reason.alarm.clone()
        });
        Decision {
            action: Action::Halt,
            announce,
        }
    }

    /// Called only after a successful sign and send: a failed signing attempt must
    /// retry next tick, so it must not be remembered as published.
    ///
    /// Deliberately says nothing to the guards. The composite judges samples as they
    /// arrive, and whether one was published is not information about the feed; a
    /// reference tied to delivery would freeze whenever nothing reached the wire, and
    /// ordinary drift measured against a stale price would then halt a lane whose feed
    /// was fine.
    pub fn published(&mut self, quote: (U256, U256)) {
        self.quoted = Some(quote);
        self.withdrawn = None;
    }

    /// The retraction itself, from either path: the header takes the reason, and the
    /// dedup cache is cleared because the quote is gone at the builders, so "unchanged"
    /// must not swallow a resumption at the very same price.
    fn retract(&mut self, reason: String) {
        self.withdrawn = Some(reason);
        self.quoted = None;
    }

    /// What this block has standing, for its header line.
    pub fn quoted(&self) -> Option<(U256, U256)> {
        self.quoted
    }

    /// Why this block is withdrawn, for its header line.
    pub fn withdrawn_reason(&self) -> Option<&str> {
        self.withdrawn.as_deref()
    }

    /// Neither of the ordinary paths announces anything: a publish and a withdraw are the
    /// steady state, and the per-block summary already reports them. Only a trip has a
    /// transition worth a line of its own, and it builds its own `Decision` in `halt`.
    fn publish(&mut self, quote: (U256, U256)) -> Decision {
        let action = if self.quoted == Some(quote) {
            Action::Nothing
        } else {
            Action::Publish(quote)
        };
        Decision {
            action,
            announce: None,
        }
    }

    /// The reason is built only when an episode starts: an unusable price arrives on every
    /// tick of an outage, and rendering an error chain to a String that the `Nothing`
    /// branch then discards is a cost the old inline code did not pay.
    fn withdraw(&mut self, reason: impl FnOnce() -> String) -> Decision {
        let action = if self.withdrawn.is_none() {
            self.retract(reason());
            Action::Withdraw
        } else {
            Action::Nothing
        };
        Decision {
            action,
            announce: None,
        }
    }
}

/// The per-block loop: stamp, sign once, publish, wait for the block to pass, then read
/// the lane back to see whether the update actually landed — a builder ack is not
/// inclusion.
#[allow(clippy::too_many_arguments)]
async fn drive(
    client: &EthClient,
    live: &Live,
    params: &UpdateParams,
    cmd: &watch::Sender<Command>,
    events: &mut mpsc::Receiver<Event>,
    names: &[String],
    opts: &SendOpts,
    head: &mut watch::Receiver<Head>,
    mut cancel: Shutdown,
) -> Result<Ended> {
    let label = &live.pair.label;
    // Named on every line that says something went wrong: the label says which pair, the
    // address says which key to fund, authorize or look up. It is what preflight's report
    // prints too, so the two can be matched without re-reading the config.
    let signer_address = live.pair.signer.address();
    let mut source = live.values.clone();
    let band = &live.pair.band;
    // How long to wait before retrying a failed fetch: the head watcher's own cadence,
    // which is the rate this loop already polls the chain at.
    let retry_delay = head::poll_interval(opts.requote_ms);
    let mut carried = BlockSummary::new(0, names);
    let mut landings = LandingTracker::default();
    let mut blocks_seen = 0u64;
    // Shared with whatever restarts this loop, so the latch is not reset by the failure it
    // is meant to see through. Still per pair, so a trip withdraws this lane's quote and
    // leaves the others quoting.
    let mut gate = PublishGate::new(live.latch.clone());
    // The latest block, as the head watcher knows it, is the first target's parent.
    // Nothing is fetched for it: the watcher delivers the header with the number.
    let mut parent = tokio::select! {
        latest = head.wait_for(|head| head.number > 0) => {
            *latest.map_err(|_| eyre!("the head watcher is gone"))?
        }
        _ = cancel.wait() => {
            // "stopping", not "shutting down": the same stop ends a lane a reload removed or
            // halted while the process goes on.
            tracing::info!("[{label}] stopping");
            return Ok(Ended::Finished);
        }
    };
    // Nothing stands while this waits: the tasks start Idle, and every later fetch is
    // preceded by an Idle publish (see the end of the loop). Raced against shutdown here
    // and at the end of the loop, because the fetch retries through an outage and an
    // operator stopping the service during one must not have to wait it out.
    let mut inputs = tokio::select! {
        inputs = fetch_block_inputs(
            client,
            label,
            &live.metrics,
            signer_address,
            true,
            retry_delay,
        ) => inputs,
        _ = cancel.wait() => {
            tracing::info!("[{label}] stopping");
            return Ok(Ended::Finished);
        }
    };

    loop {
        let BlockInputs { nonce, balance } = inputs;
        let target_block = parent.number + 1;
        // A failed derivation (no base fee, ts overflow) is a chain property that will
        // never heal, not an RPC blip: fail hard rather than retrying forever. Mutable
        // for one reason: a missed slot moves the stamp on (see the select! below).
        let mut stamp = UpdateStamp::from_head(&parent)?;

        // Preflight priced the runway once, at startup; a key drains during the run,
        // which is exactly when nothing was watching it. Priced off the parent's own base
        // fee, so the runway reflects what an update actually costs (these transactions
        // pay no priority fee) rather than a tip-inclusive gas price estimate.
        if let Some(balance) = balance {
            // One pricing of the key, feeding both the line and the gauges. Written
            // unconditionally: a check that could not read the balance must overwrite the
            // last one's numbers rather than leave them standing. See `runway_check`.
            let check = runway_check(
                signer_address,
                balance,
                Some(U256::from(stamp.parent_base_fee)),
            );
            if let Some(alert) = check.alert {
                tracing::warn!("[{label}] {alert}");
            }
            live.metrics.signer_balance_wei.set(check.balance_wei);
            live.metrics.signer_runway_updates.set(check.runway);
        }
        blocks_seen += 1;
        live.metrics.target_blocks.inc();

        let mut summary = carried.carry_over(target_block);
        gate.begin_block();
        let mut halted = false;
        // The latest block once this target has passed — the next target's parent —
        // assigned by the one arm below that leaves the loop with a block. `None` past
        // the loop means the halt arm broke out, which returns before the rollover.
        let mut next_parent: Option<Head> = None;
        // Fires once the wall clock has passed this stamp's slot with no block landing
        // in it (see MISSED_SLOT_SLACK). Pinned outside the select! loop so it keeps its
        // deadline across passes: select! builds each arm's future fresh on every pass
        // and drops the losers, so an inline sleep would restart from zero whenever
        // another arm won — and `source.changed()` wins sub-second on a liquid pair.
        let mut missed_slot = Box::pin(tokio::time::sleep_until(at_wall_secs(stamp.missed_at())));
        loop {
            // `decide` still takes `eyre::Result`, not `Result<_, Unusable>`. It has 38
            // call sites in this module, almost all of them `gate.decide(Ok(..))` in the
            // tests below with no error type ever named — genericizing `decide` over the
            // error would leave every one of those with an unconstrained type parameter,
            // and a free function has no default to fall back on the way a struct or trait
            // would. Converting once, here, at the single production call site, is cheaper
            // than annotating 38 tests that do not care what the error type is.
            //
            // That conversion is one-way: `Unusable::kind` does not survive it. `Into::into`
            // folds `Unusable` into an `eyre::Report`, and everything downstream of `decide`
            // — the `withdraw` path below — only ever turns that back into a `String` for
            // the withdrawn reason. Nothing past this line can recover which kind of
            // unusable price this was. Whatever needs the kind (the `price_unusable` metric)
            // has to be counted off `source.current(band)`'s own `Result`, before this
            // conversion, not off `decide` or the gate afterwards.
            let price = source.current_detailed(band);
            if let Err(err) = &price {
                err.count(&live.metrics);
            }
            // Kept beside the decision: if this tick's price is the one that goes out, the
            // recorder gets the working behind it.
            let priced = price.as_ref().ok().copied();
            let decision = gate.decide(price.map(|p| (p.delta, p.mid)).map_err(Into::into));
            // One counter over all four actions, recorded on every tick regardless of which
            // arm below runs: the count of `Nothing` ticks is itself useful (a lane sitting
            // idle vs. one that stopped emitting entirely), and a per-arm `inc()` scattered
            // across the match below would miss that branch.
            live.metrics
                .publish_decisions(action_label(&decision.action))
                .inc();
            // stderr, like every other alarm here (runway, restarts, the all-down notice):
            // the only announcement this gate makes is a trip, and an operator whose
            // alerting tails stderr must not have the one line naming the pair that
            // stopped — and why — go to stdout with the routine block summaries.
            if let Some(line) = decision.announce {
                tracing::warn!("[{label}] {line}");
            }
            match decision.action {
                Action::Halt => {
                    // The transition (`breaker_tripped`, `breaker_trips`, `trip_source`) was
                    // recorded by the latch when it tripped, whoever tripped it; nothing to
                    // record here, and recording here too would count a trip twice.
                    // The withdraw goes out immediately: the quote must be gone at the
                    // builders whatever happens to this task next.
                    let _ = cmd.send(Command::Withdraw {
                        block_number: target_block,
                    });
                    // Deliberately `break`, not `return`. Returning from here skips the
                    // event drain and the block summary below, so the withdrawn reason the
                    // gate just set would never be printed and the operator would never
                    // learn whether any builder acked the cancel — on the one block where
                    // that is the most important thing to know.
                    halted = true;
                    break;
                }
                Action::Publish(quote) => {
                    match sign_update_tx(client, &live.pair.signer, params, stamp, nonce, quote)
                        .await
                    {
                        Ok(raw_tx) => {
                            gate.published(quote);
                            // Set only here, after a successful sign and send: a failed
                            // attempt retries next tick with the same quote (see
                            // `gate.published`'s own doc), and setting the gauge on a
                            // failure would claim a price went out that never did.
                            live.metrics
                                .published_delta
                                .set(crate::feed::scaled_to_f64(quote.0, opts.price_decimals));
                            live.metrics
                                .published_mid
                                .set(crate::feed::scaled_to_f64(quote.1, opts.price_decimals));
                            // What went out and what it was priced from, for the recorder,
                            // with the pricer's diagnostics as of this tick.
                            if let Some(recorder) = &opts.recorder {
                                let to_f64 =
                                    |v: U256| crate::feed::scaled_to_f64(v, opts.price_decimals);
                                let feed_mid = priced
                                    .filter(|p| (p.delta, p.mid) == quote)
                                    .map_or(quote.1, |p| p.feed_mid);
                                recorder.quote(crate::record::QuoteRow {
                                    prop_amm: opts.target,
                                    lane: live.pair.lane,
                                    label: label.to_string(),
                                    block: target_block,
                                    ts: wall_clock_secs() as i64,
                                    feed_mid: to_f64(feed_mid),
                                    published_mid: to_f64(quote.1),
                                    delta: to_f64(quote.0),
                                    terms: crate::record::terms_from(&source.diagnostics_now()),
                                });
                            }
                            let _ = cmd.send(Command::Quote(Arc::new(Quote {
                                raw_tx,
                                block_number: target_block,
                            })));
                        }
                        Err(err) => {
                            live.metrics.sign_errors.inc();
                            tracing::warn!(
                                "[{label}] failed to sign update with {signer_address:#x} \
                                 (will retry): {err:#}"
                            );
                        }
                    }
                }
                Action::Withdraw => {
                    // The builders' own tickers keep the retraction in force.
                    let _ = cmd.send(Command::Withdraw {
                        block_number: target_block,
                    });
                }
                Action::Nothing => {}
            }

            let source_stale = source.source_stale();
            tokio::select! {
                // The shared shutdown signal rather than a listener of this loop's own:
                // one that only exists while the loop runs leaves a pair waiting out a
                // restart backoff with nothing listening. See supervisor::Shutdown.
                _ = cancel.wait() => {
                    tracing::info!("[{label}] stopping");
                    return Ok(Ended::Finished);
                }
                // Event-driven: re-sign exactly when the feed moves, never on a poll.
                _ = source.changed() => {}
                // The primary source aging past `max_source_age_ms` with nothing else moving:
                // the next pass withdraws. Pending for a pair without that check.
                _ = source_stale => {}
                // A trip from outside this loop (the composite task's guards, a panic in a
                // component's task, a kill switch) halts on this tick, not on the next
                // price move: the next `decide` reads the latch first. Resolves at once
                // when the latch is already tripped, so a loop that starts between a trip
                // and its first price move halts without waiting for either.
                _ = live.latch.notified() => {}
                Some(event) = events.recv() => {
                    if let Some(line) = summary.fold(&event) {
                        tracing::info!("[{label}] {line}");
                    }
                }
                // The head is observed, never polled from here: a poll is an RPC
                // round-trip, and awaited in this loop it held every other arm — a price
                // move included — for that long on every tick (see head.rs). `wait_for`
                // checks the current value before waiting, so a block that landed while
                // the loop was busy signing is seen on the very next pass. Recreated per
                // pass without loss: the version lives in the channel, not the future.
                passed = head.wait_for(|head| head.number >= target_block) => {
                    // The watcher's tasks never exit on their own; a closed channel means
                    // they panicked, which nothing here can repair.
                    next_parent = Some(*passed.map_err(|_| eyre!("the head watcher is gone"))?);
                    break;
                }
                // The slot this quote was stamped for has passed with no block in it.
                // The next block will carry the following slot's timestamp, and the
                // registry accepts an update only in a block stamped exactly as it is,
                // so what the builders hold can no longer land: re-stamp for the slot
                // still ahead and re-sign. The block number is unchanged — a missed slot
                // does not skip one — so the same target and nonce stay right.
                _ = &mut missed_slot => {
                    let restamped = stamp.after_missed_slots(wall_clock_secs())?;
                    if restamped.ts != stamp.ts {
                        tracing::info!(
                            "[{label}] slot at {} passed without a block; restamping block \
                             {target_block} for {}",
                            stamp.ts, restamped.ts
                        );
                        stamp = restamped;
                        // Forces a re-sign at the top of the loop. A withdrawn quote stays
                        // withdrawn: there is nothing to re-stamp until the price is usable.
                        gate.resign();
                    }
                    // Whole seconds on both sides (at_wall_secs and after_missed_slots),
                    // so a deadline that fired a hair early is re-armed at least a second
                    // ahead rather than spun on.
                    missed_slot.as_mut().reset(at_wall_secs(stamp.missed_at()));
                }
            }
        }

        // The block has passed and its quote died with it, so the tasks go idle rather
        // than spending a cancel on it. Published here, before anything below can take a
        // round-trip: the read-back and the next block's fetches both wait on the RPC, and
        // the fetches retry through an outage, so an Idle that sat after them would leave
        // this block's `Command::Quote` standing in the channel — resent by every builder
        // task for as long as the RPC took. Publishing Idle first means no amount of RPC
        // waiting can leave a dead quote standing.
        //
        // Never on a halt: the halt arm just published `Command::Withdraw`, and a watch
        // channel keeps only its latest value, so an Idle sent here — with no await in
        // between for the builder tasks to run — would overwrite the cancel before any of
        // them saw it. The Withdraw stands instead; writing it clears each task's
        // `live_block`, so nothing is re-sent after the one cancel.
        if !halted {
            let _ = cmd.send(Command::Idle);
        }

        // Fold in whatever is still queued before printing this block's line.
        while let Ok(event) = events.try_recv() {
            if let Some(line) = summary.fold(&event) {
                tracing::info!("[{label}] {line}");
            }
        }
        // On a halt, then wait — bounded — for the builders to report the cancel.
        // `try_recv` alone cannot see it: sending `Command::Withdraw` is a synchronous
        // watch write and the code from there to here contains no await, so the builder
        // tasks have not run yet and the block line would report each builder's *previous*
        // answer — a stale `ack` under a `withdrawn (price deviation …)` header. Drained
        // first so an ack that was already queued when the trip fired is on the record
        // before the wait decides whether anyone is still expected to answer. The wait is
        // only for the line; the withdraw itself is guaranteed by `run`'s wind-down
        // whatever this collects.
        //
        // Waits while any builder *could* still answer, not only while one has acked this
        // block: a trip commonly fires on a block's first tick, when `carry_over` has just
        // reset every column to pending, yet a builder still active from the last block
        // does withdraw and does report it. Builders that are down or already withdrawn
        // end the wait early; otherwise it runs the full grace, once, at the end of the
        // pair's life.
        if halted {
            let deadline = tokio::time::Instant::now() + HALT_REPORT_GRACE;
            while summary.awaiting_cancels() {
                let Ok(Some(event)) = tokio::time::timeout_at(deadline, events.recv()).await else {
                    break;
                };
                if let Some(line) = summary.fold(&event) {
                    tracing::info!("[{label}] {line}");
                }
            }
        }
        let rendered = match (gate.quoted(), gate.withdrawn_reason()) {
            (Some((delta, mid)), _) => Quoted::Slots {
                delta,
                mid,
                ts: stamp.ts,
            },
            (None, Some(reason)) => Quoted::Withdrawn(reason.to_owned()),
            // Reached both when the feed never produced a price and when it did but every
            // signing attempt failed, so the reason must not blame the feed for either.
            (None, None) => Quoted::Withdrawn("nothing signed for this block".to_owned()),
        };
        for line in summary.render(&rendered).lines() {
            tracing::info!("[{label}] {line}");
        }
        // Once per block, what the summary line says, for the observers: never per tick.
        // Not on a halt, which broke out before `target_block` was mined (see below): the
        // event says a block passed, and a reload that re-arms the lane within the slot
        // would have its new generation say the same block passed, the other way. The
        // observers hear of the halt from the latch's `Tripped` and the `LaneStopped`.
        if !halted {
            live.observers.emit(crate::observe::Event::Block {
                pair: label.to_string(),
                block: target_block,
                outcome: match &rendered {
                    Quoted::Slots { delta, mid, .. } => crate::observe::BlockOutcome::Quoted {
                        delta: *delta,
                        mid: *mid,
                    },
                    Quoted::Withdrawn(reason) => crate::observe::BlockOutcome::Withdrawn {
                        reason: reason.clone(),
                    },
                },
                diagnostics: source.diagnostics_now(),
            });
        }

        // Announced here, before the landing check, because there is no landing to check:
        // the loop broke out of this block early, so `target_block` has not been mined.
        // Verifying against it would `eth_call` a block that does not exist yet, print
        // "block N passed" for a block that has not, and book a spurious miss.
        if halted {
            tracing::warn!(
                "[{label}] halted; this pair will not quote again until you reload ({}) once \
                 the feed is trusted",
                crate::hints::reload()
            );
            return Ok(Ended::Halted);
        }

        // Whether anything actually reached the chain. At least one builder's latest
        // answer being an ack of a non-empty transaction is the condition under which a
        // landing was expected at all; without it the block says nothing about the lane.
        let verify = async {
            if summary.any_acked() {
                verify_landed(
                    client,
                    &live.metrics,
                    opts,
                    params.lane,
                    stamp.ts,
                    target_block,
                )
                .await
            } else {
                Landing::NotQuoted
            }
        };
        // This block's read-back and the next block's inputs depend on nothing but the
        // RPC, so they go out together: sequenced, each round-trip here was added to the
        // time the new block spent with no quote standing. Under --once there is no next
        // block, and a fetch that retries through an outage must not keep it from exiting
        // — nor keep a shutdown waiting, hence the race against `cancel`.
        let next = async {
            if opts.once {
                return None;
            }
            tokio::select! {
                inputs = fetch_block_inputs(
                    client,
                    label,
                    &live.metrics,
                    signer_address,
                    blocks_seen.is_multiple_of(RUNWAY_CHECK_BLOCKS),
                    retry_delay,
                ) => Some(inputs),
                _ = cancel.wait() => None,
            }
        };
        let (landing, next) = tokio::join!(verify, next);
        // One counter over all four outcomes, recorded here rather than in `verify_landed`:
        // `NotQuoted` never reaches that function, so counting inside it would leave the
        // one outcome that means "nothing was on the wire" unrepresented.
        live.metrics.landings(landing_label(landing)).inc();
        // The lane's own read-back, for a pricer that adapts to what happened to its quotes
        // (`BuildCtx::landings`): reported once the outcome is known, whatever it was.
        live.values.report_landing(crate::pricing::LandingReport {
            block: target_block,
            landing: landing.into(),
            at: std::time::Instant::now(),
        });
        live.observers.emit(crate::observe::Event::Landing {
            pair: label.to_string(),
            block: target_block,
            outcome: landing.into(),
        });
        // Only a landing is worth a line. Most blocks have no update in them, because a
        // builder includes it only when a swap hits this pair in the block it builds, and a
        // line per such block would drown the ones that matter. The other outcomes are
        // counted in the metric above, and a read-back that keeps failing is reported by
        // the tracker below.
        if landing == Landing::Landed {
            tracing::info!("[{label}] block {target_block}: update landed");
        }
        if let Some(alert) = landings.record(landing) {
            tracing::warn!("[{label}] {alert}; signer {signer_address:#x}");
        }
        live.metrics
            .consecutive_landing_misses
            .set(landings.misses() as f64);
        carried = summary;
        let Some(next) = next else {
            // --once, or a shutdown that interrupted the fetch.
            if cancel.is_set() {
                tracing::info!("[{label}] stopping");
            }
            return Ok(Ended::Finished);
        };
        inputs = next;
        // The halt arm returned above, so the only break that reaches here is the head
        // arm's, which always assigns the parent.
        parent = next_parent.expect("left the block loop with neither a halt nor a new head");
    }
}

/// The tokio deadline for a wall-clock second, as an offset from now. Whole seconds,
/// deliberately: `after_missed_slots` reads the wall clock in whole seconds too, so a
/// deadline computed here never fires before that check would agree the slot is missed.
fn at_wall_secs(secs: u64) -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(secs.saturating_sub(wall_clock_secs()))
}

/// What one target block needs of its signer: the pending nonce, and — every
/// RUNWAY_CHECK_BLOCKS — the balance. The parent block itself comes from the head
/// watcher, header included, so nothing here fetches it.
struct BlockInputs {
    nonce: u64,
    /// `Some` when the runway was due for a check. The inner `None` is a balance that
    /// could not be read, which the alert reports rather than passes over.
    balance: Option<Option<U256>>,
}

/// Fetches a block's signer inputs, together, since neither depends on the other.
///
/// The nonce is read once per block, not once per re-sign: this pair's nonce cannot
/// change while it quotes for one block, since nothing of its own has landed yet, and if
/// the previous block's update did land, this fetch captures it. That keeps the requote
/// budget free of an RPC round-trip. Read from the pending block, not latest (the SDK's
/// default), so a transaction of ours the pool still holds is counted.
///
/// Retried every `retry_delay` rather than fatal: a blip on eth_getTransactionCount must
/// not take the service down. Safe to retry indefinitely because every caller publishes
/// `Command::Idle` first, so nothing stale stands while this waits, and every caller
/// races it against shutdown. Each read is bounded by RPC_TIMEOUT, so a hung RPC is
/// retried like a failed one rather than waited on forever. A balance that fails to read
/// is not retried — the runway alert reports it.
async fn fetch_block_inputs(
    client: &EthClient,
    label: &str,
    metrics: &PairMetrics,
    signer_address: Address,
    check_runway: bool,
    retry_delay: Duration,
) -> BlockInputs {
    // The balance is read at most once per call, whatever the nonce does. `Some` here means
    // this check has already had its one attempt — `Some(None)` included, which is an
    // attempt that failed.
    //
    // Load-bearing for PusherSignerRunwayUnknown, not just tidiness. The nonce retries every
    // `retry_delay` (250ms by default), and a balance read inside that loop turns
    // `rpc_errors_total{call="get_balance"}` into a count of *attempts*: an RPC outage
    // accumulated three of them in under a second, so a rule whose annotation promises
    // "has not been priced in ~30 minutes" fired at the next 15s scrape. Its threshold is
    // three failed *checks*, and one check now emits at most one error. It also stops the
    // loop hammering an already-failing endpoint with a second call it has no use for.
    let mut attempted: Option<Option<U256>> = None;
    loop {
        let want_balance = check_runway && attempted.is_none();
        let balance = async {
            if !want_balance {
                return None;
            }
            Some(
                timed(
                    metrics,
                    RpcCall::GetBalance,
                    bounded(
                        client.get_balance(signer_address, BlockIdentifier::Tag(BlockTag::Latest)),
                    ),
                )
                .await
                .ok(),
            )
        };
        // Timed inside the join, per call, rather than around it: the two overlap, so a
        // timer around the pair would report the slower of them as the cost of both and
        // hide which one is slow. `get_balance`'s error counter is also what
        // PusherSignerRunwayUnknown reads, and it must count a failed balance read even
        // though the nonce beside it succeeded.
        let (nonce, balance) = tokio::join!(
            timed(
                metrics,
                RpcCall::GetNonce,
                bounded(client.get_nonce(signer_address, BlockIdentifier::Tag(BlockTag::Pending))),
            ),
            balance,
        );
        if let Some(balance) = balance {
            attempted = Some(balance);
        }
        match nonce {
            Ok(nonce) => {
                return BlockInputs {
                    nonce,
                    balance: attempted,
                };
            }
            Err(err) => tracing::warn!(
                "[{label}] failed to fetch the nonce of {signer_address:#x} (will retry): \
                 {err:#}"
            ),
        }
        tokio::time::sleep(retry_delay).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::mock,
        config::{MidBand, Pair, lane_of},
        feed::PriceSample,
        metrics::LandingResult,
        rpc_mock::MockRpc,
        supervisor,
        update::ValueSource,
    };
    use ethrex_l2_rpc::signer::{LocalSigner, Signer};
    use secp256k1::SecretKey;

    /// A throwaway registry: this test only needs somewhere for the pair's counters to land.
    fn test_pair_metrics() -> crate::metrics::PairMetrics {
        crate::metrics::Metrics::new().unwrap().for_pair("TEST")
    }

    /// The same, for the head watcher a `drive` test has to start. Its own registry: these
    /// tests assert on the pair's series, never the watcher's, which head.rs covers.
    fn head_metrics() -> crate::metrics::HeadMetrics {
        crate::metrics::Metrics::new()
            .unwrap()
            .register_head()
            .unwrap()
    }

    /// Pins the label layout `for_builder` produces: `BuilderTask` depends on `builder` and
    /// `pair` never colliding across two different builders on the same pair, or the same
    /// builder on two different pairs. If this ever fails, the label order in `for_builder`
    /// changed, not the recording sites below it.
    #[test]
    fn builder_counters_are_independent_per_builder_and_pair() {
        let m = crate::metrics::Metrics::new().unwrap();
        m.for_builder("USDC/USDT", "titan-eu").acks.inc();
        m.for_builder("USDC/USDT", "titan-us").rejections.inc();
        m.for_builder("WETH/USDC", "titan-eu").acks.inc();
        let text = m.render().unwrap();
        assert!(
            text.contains(
                r#"quote_updater_builder_acks_total{builder="titan-eu",pair="USDC/USDT"} 1"#
            ),
            "{text}"
        );
        assert!(
            text.contains(
                r#"quote_updater_builder_acks_total{builder="titan-eu",pair="WETH/USDC"} 1"#
            ),
            "{text}"
        );
        assert!(
            text.contains(
                r#"quote_updater_builder_rejections_total{builder="titan-us",pair="USDC/USDT"} 1"#
            ),
            "{text}"
        );
    }

    fn identity() -> QuoteIdentity {
        QuoteIdentity::new(
            (
                Address::from_slice(&[0xa0; 20]),
                Address::from_slice(&[0xda; 20]),
            ),
            Address::from_slice(&[0x77; 20]),
        )
    }

    fn quote(block_number: u64, raw_tx: Vec<u8>) -> Arc<Quote> {
        Arc::new(Quote {
            raw_tx,
            block_number,
        })
    }

    fn names() -> Vec<String> {
        vec![
            "titan".to_owned(),
            "buildernet".to_owned(),
            "quasar".to_owned(),
        ]
    }

    fn event(name: &str, block: Option<u64>, kind: EventKind) -> Event {
        Event {
            name: name.to_owned(),
            block_number: block,
            kind,
        }
    }

    /// An event sequence as (kind, block), so an assertion reads as the story it tells
    /// and a failure prints one.
    fn story(events: &[Event]) -> Vec<(&'static str, Option<u64>)> {
        events
            .iter()
            .map(|event| {
                let kind = match event.kind {
                    EventKind::Acked { .. } => "acked",
                    EventKind::Withdrawn => "withdrawn",
                    EventKind::Rejected { .. } => "rejected",
                    EventKind::Disconnected { .. } => "disconnected",
                    EventKind::Reconnected => "reconnected",
                };
                (kind, event.block_number)
            })
            .collect()
    }

    /// How many empty-tx updates this endpoint has seen.
    fn withdraws(endpoint: &mock::Mock) -> usize {
        endpoint
            .received()
            .iter()
            .filter(|update| update.tx.is_empty())
            .count()
    }

    /// Polls `cond` until it holds, or `within` elapses.
    ///
    /// For assertions about builder counters, which are recorded when the builder's
    /// *response* is read (see `on_ack`) rather than when the update is written. Sends do not
    /// wait for their acks, so a test that has observed only the mock's receipt is up to one
    /// response ahead of the count, and comparing the two right there is a race the fast
    /// machine wins and a loaded CI runner loses. Waiting for them to meet is not a
    /// concession to slowness: the count genuinely arrives later now, and the wire tally is
    /// the wrong clock to read it against.
    ///
    /// The bound is what keeps a real regression — a counter moved inside the per-block
    /// report throttle, say — a failure rather than a hang.
    async fn wait_until(within: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        cond()
    }

    /// Waits for an empty-tx update to arrive. `Mock::wait_for` counts messages, which
    /// cannot express this: requotes keep arriving at the requote cadence, so waiting for
    /// "one more message" is satisfied by the next requote well before the withdraw lands.
    async fn wait_for_withdraw(endpoint: &mock::Mock, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            while withdraws(endpoint) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    /// Drives a set of builder tasks the way the main loop does. Nothing here touches a
    /// chain or an RPC, because a builder task talks only to its builder — the isolation
    /// that keeps one slow builder from hurting the others is the same isolation that
    /// makes this testable.
    struct Harness {
        cmd: watch::Sender<Command>,
        stop: watch::Sender<bool>,
        events: mpsc::Receiver<Event>,
        tasks: Vec<(String, tokio::task::JoinHandle<()>)>,
        /// Each builder's metric handle, the same one its `BuilderTask` holds — so a test
        /// reading these back sees exactly what the recording sites set, not a copy.
        metrics: Vec<(String, crate::metrics::BuilderMetrics)>,
    }

    impl Harness {
        fn spawn(builders: &[(&str, &str)], requote: Duration) -> Self {
            Self::spawn_all(
                builders.iter().map(|(name, url)| (*name, *url, None)),
                requote,
            )
        }

        /// One builder, starting from a connection the test established itself. That is
        /// the shape `run` hands `BuilderTask::new` — a builder that connects at
        /// startup arrives with `conn: Some(..)` and no dial of its own — so it needs
        /// coverage separate from the dial-it-yourself shape every other test uses.
        fn spawn_connected(name: &str, url: &str, requote: Duration, conn: BuilderClient) -> Self {
            Self::spawn_all(std::iter::once((name, url, Some(conn))), requote)
        }

        fn spawn_all<'a>(
            builders: impl Iterator<Item = (&'a str, &'a str, Option<BuilderClient>)>,
            requote: Duration,
        ) -> Self {
            let (cmd_tx, cmd_rx) = watch::channel(Command::Idle);
            let (stop_tx, stop_rx) = watch::channel(false);
            let (ev_tx, events) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
            // One throwaway registry for the whole harness: the `(builder, pair)` label
            // pair already keeps every builder's counters apart (see
            // `builder_counters_are_independent_per_builder_and_pair`), so a single
            // registry is enough and keeps `metrics_for` looking up the very handle each
            // `BuilderTask` writes into rather than a disconnected copy.
            let registry = crate::metrics::Metrics::new().unwrap();
            let mut metrics = Vec::new();
            let tasks = builders
                .map(|(name, url, conn)| {
                    let config = BuilderConfig {
                        name: name.to_owned(),
                        endpoint: url.to_owned(),
                        api_key: "test-key".to_owned(),
                        disable_cross_region: None,
                    };
                    let builder_metrics = registry.for_builder("TEST", name);
                    metrics.push((name.to_owned(), builder_metrics.clone()));
                    let task =
                        BuilderTask::new(config, identity(), false, requote, conn, builder_metrics);
                    (
                        name.to_owned(),
                        tokio::spawn(task.run(cmd_rx.clone(), stop_rx.clone(), ev_tx.clone())),
                    )
                })
                .collect();
            Self {
                cmd: cmd_tx,
                stop: stop_tx,
                events,
                tasks,
                metrics,
            }
        }

        /// The metric handle recorded for one builder — the same instance its
        /// `BuilderTask` holds, so asserting against it exercises the actual recording
        /// sites rather than a value the test computed independently.
        fn metrics_for(&self, name: &str) -> crate::metrics::BuilderMetrics {
            self.metrics
                .iter()
                .find(|(builder, _)| builder == name)
                .unwrap_or_else(|| panic!("no builder named {name} in this harness"))
                .1
                .clone()
        }

        fn drain(&mut self) -> Vec<Event> {
            let mut out = Vec::new();
            while let Ok(event) = self.events.try_recv() {
                out.push(event);
            }
            out
        }

        /// Collects events until `enough` holds over everything collected so far, or the
        /// timeout expires; returns what it got either way, so a failing assertion can
        /// print the sequence it actually saw. Waiting on the event stream is the only
        /// option for these two: requote traffic satisfies any wire-message count long
        /// before the event under test arrives.
        async fn collect_until(
            &mut self,
            timeout: Duration,
            enough: impl Fn(&[Event]) -> bool,
        ) -> Vec<Event> {
            let mut out = Vec::new();
            let _ = tokio::time::timeout(timeout, async {
                while !enough(&out) {
                    match self.events.recv().await {
                        Some(event) => out.push(event),
                        None => return,
                    }
                }
            })
            .await;
            out
        }

        async fn shutdown(self) {
            let _ = self.stop.send(true);
            for (_, task) in self.tasks {
                let _ = tokio::time::timeout(SHUTDOWN_GRACE, task).await;
            }
        }
    }

    // ---- Whole-pair runs: the real `run` against a mock RPC and mock builders ----

    /// A source that never moves, for tests where only the block matters.
    fn fixed_values() -> ValueSource {
        ValueSource::fixed_for_tests(U256::from(5u64), U256::exp10(18), 6)
    }

    /// One pair running the real `run`: its own signer, lane, builder connections and
    /// quote loop, talking to a [`MockRpc`] for the chain and [`mock::Mock`]s for the
    /// builders. What `pusher::run` builds per pair, minus preflight.
    struct PairRun {
        task: JoinHandle<Result<Ended>>,
        stop: watch::Sender<bool>,
        /// The head watcher this run reads from; aborted when the run is dropped.
        _head: crate::tasks::Tasks,
    }

    fn spawn_pair(rpc: &MockRpc, builders: &[&mock::Mock], values: ValueSource) -> PairRun {
        spawn_pair_with(rpc, builders, values, false)
    }

    /// `spawn_pair`, optionally with the head watcher subscribed to the mock's newHeads.
    fn spawn_pair_with(
        rpc: &MockRpc,
        builders: &[&mock::Mock],
        values: ValueSource,
        subscribe: bool,
    ) -> PairRun {
        let client = EthClient::new(Url::parse(&rpc.url).unwrap()).unwrap();
        // One registry, shared: `run` takes the whole thing (it binds the builder handles
        // itself), and `live` carries the pair handle bound off that same registry, so a
        // counter asserted through either is the same series.
        let metrics = Arc::new(crate::metrics::Metrics::new().unwrap());
        let signer = Signer::from(LocalSigner::new(SecretKey::from_slice(&[7u8; 32]).unwrap()));
        let (token0, token1) = (
            Address::from_slice(&[0xa0; 20]),
            Address::from_slice(&[0xda; 20]),
        );
        let live = Live {
            pair: Pair {
                tokens: (token0, token1),
                lane: lane_of(token0, token1),
                signer,
                label: "T0/T1".to_owned(),
                band: MidBand::default(),
            },
            values,
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: metrics.for_pair("T0/T1"),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let opts = SendOpts {
            registry: Address::from_slice(&[0x11; 20]),
            target: Address::from_slice(&[0x77; 20]),
            chain_id: 1,
            no_pin: false,
            mine: false,
            once: false,
            requote_ms: 50,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let config = Arc::new(BuildersConfig {
            builders: builders
                .iter()
                .enumerate()
                .map(|(i, m)| BuilderConfig {
                    name: format!("b{i}"),
                    endpoint: m.url.clone(),
                    api_key: "test-key".to_owned(),
                    disable_cross_region: None,
                })
                .collect(),
        });
        let (stop, cancel) = supervisor::shutdown_channel();
        let mut head_tasks = crate::tasks::Tasks::default();
        let head = head::spawn(
            &mut head_tasks,
            client.clone(),
            head::poll_interval(opts.requote_ms),
            head::POLL_TIMEOUT,
            subscribe.then(|| rpc.ws_url.clone()),
            metrics.register_head().unwrap(),
        );
        let task = tokio::spawn(run(
            client,
            live,
            opts,
            config,
            "builders.toml".to_owned(),
            head,
            cancel,
            metrics,
        ));
        PairRun {
            task,
            stop,
            _head: head_tasks,
        }
    }

    impl PairRun {
        async fn shutdown(self) {
            self.shutdown_within(Duration::from_secs(10)).await;
        }

        /// Raises the shutdown signal and requires the pair to exit cleanly within
        /// `bound`.
        async fn shutdown_within(self, bound: Duration) {
            self.stop.send_replace(true);
            let outcome = tokio::time::timeout(bound, self.task)
                .await
                .unwrap_or_else(|_| panic!("the pair must exit within {bound:?} of shutdown"))
                .expect("the pair task must not panic");
            assert!(
                outcome.is_ok(),
                "the pair exited with an error: {outcome:?}"
            );
        }
    }

    /// `EthClient` is built on a reqwest client with no request timeout, so a read the
    /// RPC never answers would otherwise hold the loop forever: no quote for the new
    /// block, and no line saying why. A hung read must be given up on and retried.
    #[tokio::test]
    async fn a_hung_parent_fetch_is_abandoned_and_retried() {
        let rpc = MockRpc::spawn(100).await;
        rpc.set_delay("eth_getBlockByNumber", Duration::from_secs(3600));
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair(&rpc, &[&b], fixed_values());
        assert!(
            rpc.wait_for_calls("eth_getBlockByNumber", 1, Duration::from_secs(5))
                .await,
            "the parent was never fetched"
        );
        // The RPC comes back: only requests from now on are answered, so a quote can
        // only appear through a retry.
        rpc.set_delay("eth_getBlockByNumber", Duration::ZERO);
        assert!(
            b.wait_for(1, crate::RPC_TIMEOUT + Duration::from_secs(3))
                .await,
            "the hung fetch was waited on instead of retried: no quote"
        );
        pair.shutdown().await;
    }

    /// The fetch retries through an outage, and the operator may want to stop the
    /// service during one. Shutdown must not wait for the RPC to come back.
    #[tokio::test]
    async fn a_shutdown_during_an_rpc_outage_does_not_wait_for_the_rpc() {
        let rpc = MockRpc::spawn(100).await;
        rpc.set_delay("eth_getBlockByNumber", Duration::from_secs(3600));
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair(&rpc, &[&b], fixed_values());
        assert!(
            rpc.wait_for_calls("eth_getBlockByNumber", 1, Duration::from_secs(5))
                .await,
            "the parent was never fetched"
        );
        pair.shutdown_within(Duration::from_secs(2)).await;
    }

    /// A feed a test moves by hand, in the shape `connect_feed` hands a pair.
    fn feed() -> (watch::Sender<Option<PriceSample>>, ValueSource) {
        let (tx, rx) = watch::channel(None);
        let values = ValueSource::feed_for_tests(rx, None, false, 18);
        (tx, values)
    }

    fn sample(mid: U256) -> PriceSample {
        PriceSample {
            delta: U256::from(5u64),
            mid,
            at: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }

    /// The head poll is an RPC round-trip. Awaited inside the quote loop it held the
    /// loop for that long on every tick, and a price move arriving meanwhile could not
    /// be re-signed until the poll came back: a slow RPC was paid for on every move. The
    /// loop must be free to sign while a poll is in flight.
    #[tokio::test]
    async fn a_price_move_is_signed_while_a_head_poll_is_in_flight() {
        let rpc = MockRpc::spawn(100).await;
        let poll = Duration::from_secs(1);
        rpc.set_delay("eth_blockNumber", poll);
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let (feed, values) = feed();
        feed.send_replace(Some(sample(U256::exp10(18))));
        let pair = spawn_pair(&rpc, &[&b], values);
        assert!(b.wait_for(1, Duration::from_secs(5)).await, "no quote");
        let first = b.received()[0].tx.clone();

        // Let a poll go out, so the move below lands while it is in flight.
        assert!(
            rpc.wait_for_calls("eth_blockNumber", 1, Duration::from_secs(5))
                .await,
            "the head was never polled"
        );
        let moved_at = std::time::Instant::now();
        feed.send_replace(Some(sample(U256::exp10(18) + U256::exp10(15))));
        let quoted_at = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(update) = b
                    .received()
                    .iter()
                    .find(|update| !update.tx.is_empty() && update.tx != first)
                {
                    return update.at;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the move was never quoted");
        let took = quoted_at.saturating_duration_since(moved_at);
        assert!(
            took < poll / 2,
            "a price move took {took:?} to reach the builder while a {poll:?} head poll \
             was in flight"
        );
        pair.shutdown().await;
    }

    /// An update is stamped one block time after its parent, and the registry accepts
    /// it only in a block carrying exactly that timestamp. When that slot passes with
    /// no block, the next block will carry the slot after it — so the quote standing at
    /// the builders can never land, and nothing here would notice until the head moved,
    /// a whole block later. The loop must re-stamp for the slot that is still ahead.
    #[tokio::test]
    async fn a_missed_slot_is_requoted_with_the_next_slots_timestamp() {
        let rpc = MockRpc::spawn(100).await;
        // A parent whose next slot is about to be given up on: its update is stamped
        // three seconds ago, so the slot counts as missed one second from now.
        let now = crate::update::wall_clock_secs();
        let parent_ts = now - 3 - crate::update::BLOCK_TIME_SECS;
        rpc.set_timestamp_of(100, parent_ts);
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair(&rpc, &[&b], fixed_values());
        assert!(b.wait_for(1, Duration::from_secs(5)).await, "no quote");

        let missed = u32::try_from(parent_ts + crate::update::BLOCK_TIME_SECS).unwrap();
        let next_slot = u32::try_from(parent_ts + 2 * crate::update::BLOCK_TIME_SECS).unwrap();
        let restamped = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(update) = b.received().iter().find(|update| {
                    !update.tx.is_empty() && crate::update::stamped_ts(&update.tx) == next_slot
                }) {
                    return update.block_number;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the quote was never re-stamped for the slot after the missed one");
        // Same target block: a missed slot does not change the next block's number.
        assert_eq!(restamped, 101);
        // And the first quote really did carry the slot that was then missed.
        assert_eq!(crate::update::stamped_ts(&b.received()[0].tx), missed);
        pair.shutdown().await;
    }

    /// Waits for the builder to receive a quote for `block_number`.
    async fn wait_for_block(b: &mock::Mock, block_number: u64, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, async {
            while !b
                .received()
                .iter()
                .any(|update| update.block_number == block_number && !update.tx.is_empty())
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    /// With a head subscription, each new block arrives with its header — the parent of
    /// the next target — so rolling over fetches no block: the only block fetch of the
    /// run is the poll's, for the head the subscription was too late for.
    #[tokio::test]
    async fn a_subscribed_pair_rolls_over_without_fetching_the_block() {
        let rpc = MockRpc::spawn(100).await;
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair_with(&rpc, &[&b], fixed_values(), true);
        assert!(b.wait_for(1, Duration::from_secs(5)).await, "no quote");
        assert!(
            rpc.wait_for_calls("eth_subscribe", 1, Duration::from_secs(2))
                .await,
            "never subscribed"
        );
        let fetched = rpc.calls("eth_getBlockByNumber").len();
        for landed in 101..=103 {
            rpc.set_head(landed);
            assert!(
                wait_for_block(&b, landed + 1, Duration::from_secs(5)).await,
                "no quote for block {}",
                landed + 1
            );
        }
        assert_eq!(
            rpc.calls("eth_getBlockByNumber").len(),
            fetched,
            "rolling over must not fetch a block the subscription already delivered"
        );
        pair.shutdown().await;
    }

    /// The runway check gets one balance read per check, however many times the nonce
    /// beside it has to be retried.
    ///
    /// PusherSignerRunwayUnknown counts `rpc_errors_total{call="get_balance"}` and treats
    /// three of them as three failed *checks* — ~30 minutes of not knowing, at
    /// RUNWAY_CHECK_BLOCKS. The balance read shares a `loop` with the nonce, which retries
    /// every 250ms, so a balance read that re-issued on every pass would turn that counter
    /// into a count of attempts: three inside a second, and a rule promising half an hour
    /// firing at the next scrape. It also spared an already-failing endpoint a call nothing
    /// was waiting for.
    #[tokio::test]
    async fn a_retried_nonce_does_not_re_read_the_balance() {
        let rpc = MockRpc::spawn(100).await;
        let client = EthClient::new(Url::parse(&rpc.url).unwrap()).unwrap();
        let metrics = test_pair_metrics();
        let signer = Address::from_slice(&[0x5a; 20]);

        // The nonce is refused outright rather than delayed: `bounded` would take
        // RPC_TIMEOUT (3s) to give up on a hung call, three seconds per retry, for a failure
        // the endpoint can hand back immediately.
        rpc.set_failing("eth_getTransactionCount", true);
        let fetch = tokio::spawn({
            let client = client.clone();
            let metrics = metrics.clone();
            async move {
                fetch_block_inputs(
                    &client,
                    "TEST",
                    &metrics,
                    signer,
                    true,
                    Duration::from_millis(20),
                )
                .await
            }
        });
        // Several retries: enough that a per-pass balance read would be unmistakable.
        assert!(
            rpc.wait_for_calls("eth_getTransactionCount", 4, Duration::from_secs(5))
                .await,
            "the nonce was not retried"
        );
        rpc.set_failing("eth_getTransactionCount", false);
        let inputs = tokio::time::timeout(Duration::from_secs(5), fetch)
            .await
            .expect("the fetch must finish once the nonce answers")
            .unwrap();

        assert!(
            inputs.balance.is_some(),
            "the check was due, so it must carry a reading"
        );
        assert_eq!(
            rpc.calls("eth_getBalance").len(),
            1,
            "one balance read per check, whatever the nonce did"
        );
        assert_eq!(
            metrics.rpc_errors(RpcCall::GetBalance).get(),
            0,
            "the balance read succeeded; only the nonce failed"
        );
        assert!(
            metrics.rpc_errors(RpcCall::GetNonce).get() >= 4,
            "every failed nonce read is still counted"
        );
    }

    /// The same, for the read that fails: one error per check, not one per retry. This is
    /// the half PusherSignerRunwayUnknown's threshold is actually built on.
    #[tokio::test]
    async fn a_failed_balance_read_is_counted_once_per_check() {
        let rpc = MockRpc::spawn(100).await;
        let client = EthClient::new(Url::parse(&rpc.url).unwrap()).unwrap();
        let metrics = test_pair_metrics();
        let signer = Address::from_slice(&[0x5b; 20]);

        rpc.set_failing("eth_getTransactionCount", true);
        rpc.set_failing("eth_getBalance", true);
        let fetch = tokio::spawn({
            let client = client.clone();
            let metrics = metrics.clone();
            async move {
                fetch_block_inputs(
                    &client,
                    "TEST",
                    &metrics,
                    signer,
                    true,
                    Duration::from_millis(20),
                )
                .await
            }
        });
        assert!(
            rpc.wait_for_calls("eth_getTransactionCount", 4, Duration::from_secs(5))
                .await,
            "the nonce was not retried"
        );
        rpc.set_failing("eth_getTransactionCount", false);
        let inputs = tokio::time::timeout(Duration::from_secs(5), fetch)
            .await
            .expect("the fetch must finish once the nonce answers")
            .unwrap();

        assert_eq!(
            inputs.balance,
            Some(None),
            "the check was due and its one attempt failed: attempted, unreadable"
        );
        assert_eq!(
            metrics.rpc_errors(RpcCall::GetBalance).get(),
            1,
            "three of these is what PusherSignerRunwayUnknown calls 30 minutes of not \
             knowing; a retry loop must not manufacture them in a second"
        );
    }

    /// Rolling to the next block needs the parent's header and the signer's nonce; the
    /// block that just passed needs its lane read back. None of the three depends on
    /// another, so they must go out together: sequenced, every RPC round-trip is added to
    /// the time the pair spends with no quote standing for the new block.
    #[tokio::test]
    async fn the_next_blocks_inputs_are_fetched_while_the_landing_check_is_in_flight() {
        let rpc = MockRpc::spawn(100).await;
        let check = Duration::from_millis(300);
        rpc.set_delay("eth_call", check);
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair(&rpc, &[&b], fixed_values());
        assert!(b.wait_for(1, Duration::from_secs(5)).await, "no quote");

        // Block 101 lands: the loop reads the lane back at 101 and fetches 101 as the
        // parent of the next target. The first eth_getBlockByNumber was the startup one.
        rpc.set_head(101);
        assert!(
            rpc.wait_for_calls("eth_getBlockByNumber", 2, Duration::from_secs(5))
                .await,
            "the next block's parent was never fetched"
        );
        // The two requests race each other onto the wire, so the check's arrival is
        // waited for too rather than assumed to have been recorded first.
        assert!(
            rpc.wait_for_calls("eth_call", 1, Duration::from_secs(5))
                .await,
            "the landing was never checked"
        );
        let check_at = rpc.calls("eth_call")[0];
        let fetch_at = rpc.calls("eth_getBlockByNumber")[1];
        let gap = fetch_at.saturating_duration_since(check_at);
        assert!(
            gap < check / 2,
            "the parent fetch waited {gap:?} for a {check:?} landing check instead of \
             overlapping it"
        );
        pair.shutdown().await;
    }

    /// Once its block has passed a quote is dead, and the builder tasks must be told so
    /// before the loop does anything that can take a round-trip — otherwise they keep
    /// resending it for as long as the landing check takes.
    #[tokio::test]
    async fn a_passed_block_is_not_requoted_while_its_landing_is_checked() {
        let rpc = MockRpc::spawn(100).await;
        let check = Duration::from_millis(400);
        rpc.set_delay("eth_call", check);
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let pair = spawn_pair(&rpc, &[&b], fixed_values());
        assert!(b.wait_for(1, Duration::from_secs(5)).await, "no quote");

        rpc.set_head(101);
        assert!(
            rpc.wait_for_calls("eth_call", 1, Duration::from_secs(5))
                .await,
            "the landing was never checked"
        );
        let check_at = rpc.calls("eth_call")[0];
        // Let the check run its course, then look at what the builder was sent meanwhile.
        // The first requote tick after the roll-over may already have been on the wire, so
        // only sends well into the check count.
        tokio::time::sleep(check).await;
        let late = b
            .received()
            .iter()
            .filter(|r| r.block_number == 101 && r.at > check_at + Duration::from_millis(100))
            .count();
        assert_eq!(
            late, 0,
            "block 101 was requoted {late} time(s) after it had passed, while its landing \
             check was in flight"
        );
        pair.shutdown().await;
    }

    /// The correctness property of the whole fan-out: every builder gets the same signed
    /// transaction, so the same nonce and the same hash make at most one inclusion
    /// possible however many builders hold it. Signing per builder would let two win the
    /// same block and produce two updateState calls.
    #[tokio::test]
    async fn every_builder_receives_byte_identical_transaction_bytes() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let mut h = Harness::spawn(&[("a", &a.url), ("b", &b.url)], Duration::from_millis(10));

        let raw_tx = vec![0x02, 0xde, 0xad, 0xbe, 0xef];
        h.cmd
            .send(Command::Quote(quote(21_500_431, raw_tx.clone())))
            .unwrap();

        assert!(a.wait_for(1, Duration::from_secs(2)).await, "a got nothing");
        assert!(b.wait_for(1, Duration::from_secs(2)).await, "b got nothing");
        let (ra, rb) = (a.received(), b.received());
        assert_eq!(ra[0].tx, raw_tx);
        assert_eq!(rb[0].tx, raw_tx);
        assert_eq!(
            (ra[0].block_number, rb[0].block_number),
            (21_500_431, 21_500_431)
        );
        // One uuid per builder, not one shared: replacement_uuid means "supersede the
        // update with this uuid", and two of our connections landing in one quote table
        // would make the lower seq counter lose every race.
        assert_ne!(ra[0].uuid, rb[0].uuid);
        let _ = h.drain();
        h.shutdown().await;
    }

    /// Idle is not a withdraw. When the target block passes, its quote dies with the
    /// block, so spending a cancel on it would be pointless traffic.
    #[tokio::test]
    async fn idle_puts_nothing_on_the_wire() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let h = Harness::spawn(&[("a", &a.url)], Duration::from_millis(10));
        // Idle is the channel's initial value; give the ticker ~20 chances to fire.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(a.count(), 0);
        h.shutdown().await;
    }

    /// Withdraw is the other half of that distinction: the block is still current and the
    /// builder is holding a quote nobody vouches for, so it takes an explicit empty-tx
    /// update to retract it — but only from a builder that actually holds one.
    #[tokio::test]
    async fn withdraw_reaches_every_builder_holding_a_quote() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let mut h = Harness::spawn(&[("a", &a.url), ("b", &b.url)], Duration::from_millis(10));
        let (m_a, m_b) = (h.metrics_for("a"), h.metrics_for("b"));

        // Nothing live yet, so a withdraw is a no-op rather than a message.
        h.cmd.send(Command::Withdraw { block_number: 5 }).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!((a.count(), b.count()), (0, 0));

        h.cmd
            .send(Command::Quote(quote(5, vec![0x02, 0x01])))
            .unwrap();
        assert!(a.wait_for(1, Duration::from_secs(2)).await);
        assert!(b.wait_for(1, Duration::from_secs(2)).await);

        h.cmd.send(Command::Withdraw { block_number: 5 }).unwrap();
        assert!(
            wait_for_withdraw(&a, Duration::from_secs(2)).await,
            "a never withdrew"
        );
        assert!(
            wait_for_withdraw(&b, Duration::from_secs(2)).await,
            "b never withdrew"
        );
        // The withdraw is also the last thing either builder hears: writing it clears
        // `live_block`, so every later tick reading the same Withdraw sends nothing.
        assert!(a.received().last().unwrap().tx.is_empty());
        assert!(b.received().last().unwrap().tx.is_empty());
        assert_eq!(withdraws(&a), 1, "one retraction is enough");
        assert_eq!(withdraws(&b), 1);

        // The wire-level retraction just confirmed above must be the counter's doing too:
        // `withdrawals` sits outside the `acked_block` throttle, so it counts without the
        // Event log having to notice a withdraw at all.
        //
        // Waited for, and waited for *before* the shutdown below: the count lands when the
        // withdraw's ack is read, so the task has to still be running to fold it. Asserting
        // after `shutdown` had joined the task made this unfixably racy — if the ack had not
        // been read by then, nothing was ever going to read it.
        assert!(
            wait_until(Duration::from_secs(5), || m_a.withdrawals.get() == 1
                && m_b.withdrawals.get() == 1)
            .await,
            "withdrawals should match the mock's own tally of one retraction each; a={}, \
             b={}",
            m_a.withdrawals.get(),
            m_b.withdrawals.get()
        );
        let _ = h.drain();
        h.shutdown().await;
    }

    /// A requote is the same quote sent again under a higher sequence number.
    #[tokio::test]
    async fn requotes_carry_a_strictly_increasing_sequence_number() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let h = Harness::spawn(&[("a", &a.url)], Duration::from_millis(10));
        h.cmd
            .send(Command::Quote(quote(7, vec![0x02, 0x09])))
            .unwrap();
        assert!(
            a.wait_for(4, Duration::from_secs(2)).await,
            "only {} sent",
            a.count()
        );

        let seqs: Vec<u64> = a.received().iter().map(|r| r.seq).collect();
        assert!(
            seqs.windows(2).all(|w| w[1] > w[0]),
            "sequence numbers must strictly increase: {seqs:?}"
        );
        // Every requote is the same quote, under one uuid.
        let uuids: Vec<Vec<u8>> = a.received().iter().map(|r| r.uuid.clone()).collect();
        assert!(uuids.windows(2).all(|w| w[0] == w[1]));
        h.shutdown().await;
    }

    /// Only the first ack per block is reported, so a 50ms cadence does not become a
    /// firehose: the steady state stays silent, as it does elsewhere in this crate.
    #[tokio::test]
    async fn only_the_first_ack_of_a_block_is_reported() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let mut h = Harness::spawn(&[("a", &a.url)], Duration::from_millis(10));
        let m = h.metrics_for("a");
        h.cmd
            .send(Command::Quote(quote(11, vec![0x02, 0x01])))
            .unwrap();
        assert!(a.wait_for(5, Duration::from_secs(2)).await);
        h.cmd
            .send(Command::Quote(quote(12, vec![0x02, 0x02])))
            .unwrap();
        assert!(a.wait_for(10, Duration::from_secs(2)).await);

        let acked: Vec<Option<u64>> = h
            .drain()
            .into_iter()
            .filter(|e| matches!(e.kind, EventKind::Acked { .. }))
            .map(|e| e.block_number)
            .collect();
        assert_eq!(acked, vec![Some(11), Some(12)]);

        // Same shape as the rejection counter below: the Event log is throttled to two lines,
        // but every requote carried a non-empty tx, so `acks` and `ack_latency` must both
        // have counted every one of them — a histogram that never observes reads identically
        // to a builder that acks instantly, so the sample count is the only way to tell
        // "recorded, fast" from "never recorded" apart.
        //
        // Measured before the shutdown rather than after it, and behind an `Idle` so the wire
        // stops moving: every one of these lands when a response is read, which the task has
        // to be alive to do. At this point no withdraw has been sent, so the wire total and
        // the ack count are the same number — the shutdown's own withdraw is asserted
        // separately below, where it belongs.
        h.cmd.send(Command::Idle).unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || m.acks.get() == a.count() as u64).await,
            "acks must count every wire ack ({}), not just the {} the per-block throttle let \
             through the Event log; counted {}",
            a.count(),
            acked.len(),
            m.acks.get()
        );
        let quotes = a.count() as u64;
        assert!(
            m.acks.get() > acked.len() as u64,
            "the counter must not inherit the Event log's per-block throttle"
        );
        assert_eq!(
            m.ack_latency.get_sample_count(),
            quotes,
            "ack_latency observed only {} samples for {quotes} acks",
            m.ack_latency.get_sample_count()
        );

        // `shutdown` withdraws the still-live quote (see
        // `shutdown_withdraws_the_live_quote_at_each_builder`), which is one more wire
        // message and the one `withdrawals` count. Waited for inside the shutdown's own
        // grace: `wind_down` sends the withdraw and then waits for its ack, so the count
        // arrives while the task is still winding down rather than after it has joined.
        h.shutdown().await;
        assert_eq!(
            m.withdrawals.get(),
            1,
            "the shutdown-time withdraw should have been counted"
        );
        let wire = a.count() as u64;
        assert_eq!(
            wire,
            quotes + 1,
            "the shutdown withdraw is the last wire message"
        );
        // `seq` is set only inside the clean-ack arm of `send` (see its comment), so in
        // this Behaviour::Ack harness — where every send is acked cleanly, including the
        // shutdown withdraw — "last sent" and "last acked" land on the same number and
        // this cannot tell them apart. What it does pin down is that the value tracks the
        // count of clean acks at all: a regression that stopped setting it, or read it off
        // the wrong field, would leave it at 0 or stuck behind `wire` while the wire count
        // keeps climbing.
        assert_eq!(
            m.seq.get(),
            wire as f64,
            "builder_seq must equal the number of acked wire messages ({wire}), including \
             the shutdown-time withdraw"
        );
    }

    /// A rejection is throttled the same way an ack is: only the first rejection per
    /// block is reported. Without this, a builder rejecting a misconfigured pool or pair
    /// on every requote would spend up to one event-channel slot per tick — 20/s at the
    /// default cadence — on a shared, bounded channel that every builder's events flow
    /// through.
    #[tokio::test]
    async fn only_the_first_rejection_of_a_block_is_reported() {
        let a = mock::spawn(mock::Behaviour::Reject("unknown pool")).await;
        let mut h = Harness::spawn(&[("a", &a.url)], Duration::from_millis(10));
        let m = h.metrics_for("a");
        h.cmd
            .send(Command::Quote(quote(11, vec![0x02, 0x01])))
            .unwrap();
        assert!(a.wait_for(5, Duration::from_secs(2)).await);
        h.cmd
            .send(Command::Quote(quote(12, vec![0x02, 0x02])))
            .unwrap();
        assert!(a.wait_for(10, Duration::from_secs(2)).await);

        let rejected: Vec<Option<u64>> = h
            .drain()
            .into_iter()
            .filter(|e| matches!(e.kind, EventKind::Rejected { .. }))
            .map(|e| e.block_number)
            .collect();
        assert_eq!(rejected, vec![Some(11), Some(12)]);

        // The property that justifies a counter separate from the `Event` log: the log above
        // is throttled to one `Rejected` per block (2 events for 2 blocks), but the counter
        // sits outside that throttle and must see every wire rejection. A counter
        // accidentally placed inside the throttle would read 2 here instead of matching the
        // wire.
        //
        // `Idle` first, so there is something to converge on: the counter moves when a
        // response is read, the mock's tally when an update is written, and while the loop
        // keeps requoting the two chase each other with the wire always a message or so
        // ahead. Idle stops the writes; the responses already in flight then land and the
        // two meet. Comparing them mid-flight is what used to pass on a fast machine and
        // fail on a loaded runner.
        h.cmd.send(Command::Idle).unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || m.rejections.get()
                == a.count() as u64)
            .await,
            "rejections must count every wire rejection ({}), not just the {} the per-block \
             throttle let through the Event log; counted {}",
            a.count(),
            rejected.len(),
            m.rejections.get()
        );
        // Strictly more than the log, stated separately: the equality above would also hold
        // if both sides were somehow 2, and this is the claim the test is named for.
        assert!(
            m.rejections.get() > rejected.len() as u64,
            "the counter must not inherit the Event log's per-block throttle"
        );
        h.shutdown().await;
    }

    /// A withdraw does not end the block. If the price recovers while the target block is
    /// still current, the fresh quote must report its own ack: not clearing `acked_block`
    /// on the withdraw leaves the event stream saying `withdrawn` and then nothing, while
    /// the builder holds a live quote — and `BlockSummary` renders each builder as a
    /// column from these events, so that column would read withdrawn while it is actively
    /// quoting.
    #[tokio::test]
    async fn a_quote_after_a_withdraw_in_the_same_block_reports_a_fresh_ack() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let mut h = Harness::spawn(&[("a", &a.url)], Duration::from_millis(10));

        h.cmd
            .send(Command::Quote(quote(9, vec![0x02, 0x01])))
            .unwrap();
        assert!(a.wait_for(1, Duration::from_secs(2)).await, "a got nothing");
        h.cmd.send(Command::Withdraw { block_number: 9 }).unwrap();
        assert!(
            wait_for_withdraw(&a, Duration::from_secs(2)).await,
            "a never withdrew"
        );
        // The same block, with a usable price again.
        h.cmd
            .send(Command::Quote(quote(9, vec![0x02, 0x02])))
            .unwrap();

        let events = h
            .collect_until(Duration::from_secs(2), |seen| {
                seen.iter()
                    .filter(|event| matches!(event.kind, EventKind::Acked { .. }))
                    .count()
                    == 2
            })
            .await;
        assert_eq!(
            story(&events),
            [
                ("acked", Some(9)),
                ("withdrawn", Some(9)),
                ("acked", Some(9)),
            ]
        );
        h.shutdown().await;
    }

    /// Coming back must be reported by a builder that was handed its connection, which is
    /// every builder that connects at startup. Deciding "was this a redial?" from whether
    /// the task had ever *dialed* makes that the one case it gets wrong: an operator would
    /// watch the builder drop and never see it recover.
    #[tokio::test]
    async fn a_builder_handed_a_live_connection_reports_its_first_reconnect() {
        // CloseAfter(1) acks one update per connection and then drops the socket, so the
        // first requote fails and the task has to redial. Its listener stays up, so the
        // redial finds the endpoint there.
        let a = mock::spawn(mock::Behaviour::CloseAfter(1)).await;
        // Connected here, not by the task: this is the shape `run` passes in.
        let conn = BuilderClient::connect(&a.url, "test-key").await.unwrap();
        let mut h = Harness::spawn_connected("a", &a.url, Duration::from_millis(10), conn);
        let m = h.metrics_for("a");
        // Set synchronously in `BuilderTask::new`, before the task's first tick, so this
        // is safe to read immediately: a builder handed a live connection must not read
        // as down before it has ever failed.
        assert_eq!(m.up.get(), 1.0, "a handed-in connection should start up");

        h.cmd
            .send(Command::Quote(quote(3, vec![0x02, 0x01])))
            .unwrap();
        let events = h
            .collect_until(Duration::from_secs(2), |seen| {
                seen.iter()
                    .any(|event| matches!(event.kind, EventKind::Reconnected))
            })
            .await;
        assert_eq!(
            story(&events),
            [
                ("acked", Some(3)),
                ("disconnected", Some(3)),
                ("reconnected", None),
            ]
        );
        // Each event above was reported (via the try_send the task itself makes) only
        // after the metric beside it was recorded, in the same synchronous span with no
        // `.await` in between it and the increment — so observing "reconnected" already
        // guarantees these are set. Read them here, before the next `.await`
        // (`h.shutdown()` below), not after it: `#[tokio::test]` defaults to a
        // single-threaded runtime, so nothing else can run until this test task yields —
        // and this builder's mock closes every connection after one ack, so the task
        // races straight into a second disconnect the moment it is next given a chance to
        // run. Reading these same three lines after `h.shutdown().await` (which does
        // yield, waiting out the join) reliably found `send_errors == 2` instead of 1.
        assert_eq!(
            m.send_errors.get(),
            1,
            "the CloseAfter(1) drop should have counted as one transport error"
        );
        assert_eq!(
            m.reconnects.get(),
            1,
            "the redial that followed should have counted as one reconnect"
        );
        assert_eq!(
            m.up.get(),
            1.0,
            "up should read connected again after the reconnect"
        );
        h.shutdown().await;
    }

    /// `send`'s transport-error arm sets `up` down beside `send_errors`. That write has an
    /// observable effect — this test proves it — but the reconnect test above cannot catch
    /// a deletion of it: it only reads `up` *after* a successful reconnect, and that
    /// reconnect resets `up` to 1.0 regardless of whether this arm ran first. This test
    /// closes the gap by reading `up` in the window between the disconnect and the redial,
    /// before a reconnect gets the chance to paper over a missing write.
    ///
    /// (`ensure_connected`'s own `Err` arm also sets `up.set(0.0)`, on a failed *dial*
    /// rather than a drop mid-stream — see its comment. That write really is unobservable,
    /// on every path that reaches it, today — a stronger and different claim than "needs
    /// the right test", and why that site has no test at all rather than one like this.)
    #[tokio::test]
    async fn a_transport_error_reports_the_builder_down_before_any_redial() {
        let a = mock::spawn(mock::Behaviour::CloseAfter(1)).await;
        let conn = BuilderClient::connect(&a.url, "test-key").await.unwrap();
        let mut h = Harness::spawn_connected("a", &a.url, Duration::from_millis(10), conn);
        let m = h.metrics_for("a");

        h.cmd
            .send(Command::Quote(quote(4, vec![0x02, 0x06])))
            .unwrap();
        let events = h
            .collect_until(Duration::from_secs(2), |seen| {
                seen.iter()
                    .any(|event| matches!(event.kind, EventKind::Disconnected { .. }))
            })
            .await;
        assert_eq!(
            story(&events),
            [("acked", Some(4)), ("disconnected", Some(4))]
        );
        // Same reasoning as the reconnect test above: read immediately, before the next
        // `.await`, so the redial this same task is about to attempt cannot have run yet.
        assert_eq!(
            m.up.get(),
            0.0,
            "up should read down as soon as the send fails"
        );
        assert_eq!(m.send_errors.get(), 1);
        h.shutdown().await;
    }

    /// The test that justifies one task per builder. A builder that accepts every write
    /// and never answers is sent MAX_UNACKED updates and then waits out ACK_TIMEOUT (5s),
    /// while builders evict a quote idle past ~400ms: in any shared send loop that stall
    /// would cost *every* builder its quote.
    #[tokio::test]
    async fn a_hung_builder_does_not_stall_a_healthy_one() {
        let healthy = mock::spawn(mock::Behaviour::Ack).await;
        let hung = mock::spawn(mock::Behaviour::Silent).await;
        let h = Harness::spawn(
            &[("healthy", &healthy.url), ("hung", &hung.url)],
            Duration::from_millis(20),
        );
        h.cmd
            .send(Command::Quote(quote(9, vec![0x02, 0x07])))
            .unwrap();

        // At a 20ms cadence, 6 updates take ~120ms — far inside the 5s the hung builder
        // is blocked for. A shared loop would deliver 1 here.
        assert!(
            healthy.wait_for(6, Duration::from_secs(2)).await,
            "healthy builder sent only {} updates while the other hung",
            healthy.count()
        );
        // The hung builder got its writes up to the cap and is waiting on acks that never
        // come: ten more ticks later it has still been sent nothing further. This holds
        // only because ACK_TIMEOUT (5s) exceeds this test's window: the wait cannot time
        // out and redial inside it. If ACK_TIMEOUT is ever lowered below that window,
        // this assertion starts failing for a reason unrelated to its name.
        assert!(
            hung.wait_for(MAX_UNACKED, Duration::from_secs(2)).await,
            "hung builder was sent only {}",
            hung.count()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(hung.count(), MAX_UNACKED);
        h.shutdown().await;
    }

    /// A send used to write and then block until its ack arrived, so a price move that
    /// landed during that wait waited too: one builder ack round-trip added to every move
    /// that happened to arrive in one. A new quote must go out the moment it exists.
    #[tokio::test]
    async fn a_new_quote_does_not_wait_for_the_previous_quotes_ack() {
        let ack = Duration::from_millis(500);
        let slow = mock::spawn(mock::Behaviour::SlowAck(ack)).await;
        // A requote interval longer than the test, so only the two quotes below are
        // ever on the wire.
        let h = Harness::spawn(&[("slow", &slow.url)], Duration::from_secs(5));
        h.cmd
            .send(Command::Quote(quote(3, vec![0x02, 0x01])))
            .unwrap();
        assert!(
            slow.wait_for(1, Duration::from_secs(2)).await,
            "the first quote never arrived"
        );
        h.cmd
            .send(Command::Quote(quote(3, vec![0x02, 0x02])))
            .unwrap();
        assert!(
            slow.wait_for(2, Duration::from_secs(2)).await,
            "the second quote never arrived"
        );
        let received = slow.received();
        let gap = received[1].at.saturating_duration_since(received[0].at);
        assert!(
            gap < ack / 2,
            "the second quote waited {gap:?} for the first one's {ack:?} ack"
        );
        h.shutdown().await;
    }

    /// Not waiting for acks is bounded: a builder that accepts every write and answers
    /// none is sent MAX_UNACKED updates and then nothing more until it answers or
    /// ACK_TIMEOUT drops the connection — not one update per tick for as long as it
    /// stays silent.
    #[tokio::test]
    async fn a_builder_that_never_acks_is_sent_no_more_than_the_unacked_cap() {
        let hung = mock::spawn(mock::Behaviour::Silent).await;
        let h = Harness::spawn(&[("hung", &hung.url)], Duration::from_millis(10));
        h.cmd
            .send(Command::Quote(quote(4, vec![0x02, 0x01])))
            .unwrap();
        // ~100 ticks: far past the cap, far short of ACK_TIMEOUT.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            hung.count(),
            MAX_UNACKED,
            "a silent builder must be sent exactly the cap, then nothing"
        );
        let metrics = h.metrics_for("hung");
        assert_eq!(
            metrics.send_errors.get(),
            0,
            "nothing has failed: the socket is fine, the builder is just quiet"
        );
        // This shutdown is the wind_down path end to end, and it costs the full ACK_TIMEOUT
        // — the withdraw goes out and its ack never comes — which is why the assertion below
        // lives in the one test already paying for that wait.
        h.shutdown().await;
        assert_eq!(
            metrics.send_errors.get(),
            0,
            "a shutdown withdraw the builder never acked is a deliberate close, not a \
             transport failure: counting it spikes this series at every restart"
        );
        assert_eq!(metrics.up.get(), 0.0, "the connection is gone either way");
    }

    /// The rule `drop_connection` enforces, in both directions and without the ACK_TIMEOUT
    /// the end-to-end path above has to wait out. `up` reports the state, so it goes to 0
    /// whatever ended the connection; `send_errors` reports an event, so only a wire that
    /// actually broke may move it.
    #[test]
    fn only_a_broken_wire_counts_as_a_send_error() {
        let registry = crate::metrics::Metrics::new().unwrap();
        let metrics = registry.for_builder("TEST", "b");
        let mut task = BuilderTask::new(
            BuilderConfig {
                name: "b".to_owned(),
                // Never dialed: `drop_connection` is pure state, so this test needs no
                // socket at all.
                endpoint: "ws://127.0.0.1:1/ws".to_owned(),
                api_key: "test-key".to_owned(),
                disable_cross_region: None,
            },
            identity(),
            false,
            Duration::from_millis(10),
            None,
            metrics.clone(),
        );

        metrics.up.set(1.0);
        task.drop_connection(Disconnect::Deliberate);
        assert_eq!(
            metrics.send_errors.get(),
            0,
            "a socket closed on purpose is not a transport failure"
        );
        assert_eq!(metrics.up.get(), 0.0, "but it is still gone");

        metrics.up.set(1.0);
        task.drop_connection(Disconnect::Failed);
        assert_eq!(
            metrics.send_errors.get(),
            1,
            "a wire that broke is exactly what this counter is for"
        );
        assert_eq!(metrics.up.get(), 0.0);
    }

    /// A reconnect must not restart the sequence counter. The builder's rule is "strictly
    /// increasing for the same uuid", so a counter back at 0 while it still remembers our
    /// old quote has every update ignored as stale until it climbs past the high-water
    /// mark. The counter therefore lives in the task, not the connection.
    #[tokio::test]
    async fn sequence_numbers_keep_increasing_across_a_reconnect() {
        // Acks two updates per connection, then closes: the task must redial and carry on.
        let m = mock::spawn(mock::Behaviour::CloseAfter(2)).await;
        let h = Harness::spawn(&[("flaky", &m.url)], Duration::from_millis(20));
        h.cmd
            .send(Command::Quote(quote(13, vec![0x02, 0x03])))
            .unwrap();

        // The mock records an update before deciding whether to ack it, so `CloseAfter(2)`
        // logs three per connection (two acked, one recorded then dropped). Reaching 5
        // therefore proves a second connection — one redial, which is all the assertions
        // below need.
        assert!(
            m.wait_for(5, Duration::from_secs(10)).await,
            "only {} updates arrived across reconnects",
            m.count()
        );
        let seqs: Vec<u64> = m.received().iter().map(|r| r.seq).collect();
        assert!(
            seqs.windows(2).all(|w| w[1] > w[0]),
            "sequence numbers reset across a reconnect: {seqs:?}"
        );
        // Same uuid throughout: it identifies the quote, not the connection.
        let uuids: Vec<Vec<u8>> = m.received().iter().map(|r| r.uuid.clone()).collect();
        assert!(uuids.windows(2).all(|w| w[0] == w[1]));
        h.shutdown().await;
    }

    /// A dead endpoint must not be redialed on every tick, nor emit a line every time.
    /// RECONNECT_DELAY is 2s, so ~50 ticks over half a second must still add up to exactly
    /// one connect attempt — `connect_attempts` is the independent signal of that, since
    /// identical errors also being suppressed means the event stream alone cannot tell a
    /// throttled single attempt from an unthrottled flood that all failed the same way.
    #[tokio::test]
    async fn a_dead_builder_is_redialed_on_a_delay_and_reported_once() {
        let dead = mock::spawn(mock::Behaviour::DropConnection).await;
        let mut h = Harness::spawn(&[("dead", &dead.url)], Duration::from_millis(10));
        h.cmd
            .send(Command::Quote(quote(3, vec![0x02, 0x05])))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(600)).await;

        assert_eq!(
            dead.connect_attempts(),
            1,
            "expected one connect attempt under RECONNECT_DELAY, got {}",
            dead.connect_attempts()
        );
        let disconnects = h
            .drain()
            .into_iter()
            .filter(|e| matches!(e.kind, EventKind::Disconnected { .. }))
            .count();
        assert_eq!(disconnects, 1, "expected one report, got {disconnects}");
        h.shutdown().await;
    }

    /// A config where nothing connects is nearly always a typo or a revoked key, so
    /// refuse to start — but say how many were tried, so a partial outage reads
    /// differently from a broken file.
    #[test]
    fn zero_connected_builders_is_a_startup_error() {
        let err = ensure_any_connected("./builders.toml", 3, 0)
            .unwrap_err()
            .to_string();
        assert!(err.contains("./builders.toml"), "{err}");
        assert!(
            err.contains('3'),
            "the error must say how many were tried: {err}"
        );
        // One is enough: a single builder's outage must not stop the others.
        assert!(ensure_any_connected("./builders.toml", 3, 1).is_ok());
        assert!(ensure_any_connected("./builders.toml", 3, 3).is_ok());
    }

    #[test]
    fn the_startup_table_names_every_builder_and_its_outcome() {
        let builders = vec![
            BuilderConfig {
                name: "titan".into(),
                endpoint: "wss://eu.rpc.titanbuilder.xyz/ws/sendquoteupdate".into(),
                api_key: "SENTINEL-KEY-MUST-NOT-LEAK".into(),
                disable_cross_region: None,
            },
            BuilderConfig {
                name: "buildernet".into(),
                endpoint: "wss://relay.buildernet.org/ws/sendquoteupdate".into(),
                api_key: "SENTINEL-KEY-MUST-NOT-LEAK".into(),
                disable_cross_region: None,
            },
        ];
        let table = startup_table(
            "./builders.toml",
            &builders,
            &[None, Some("connect timed out".into())],
        );
        assert!(table.contains("2 configured from ./builders.toml"));
        assert!(table.contains("titan"));
        assert!(table.contains("connected"));
        assert!(table.contains("buildernet"));
        assert!(table.contains("connect timed out"));
        assert!(table.contains("1/2 builders live"));
        // An API key must never reach a log line, and this is the only place that formats
        // a builder's config for printing.
        assert!(
            !table.contains("SENTINEL-KEY-MUST-NOT-LEAK"),
            "the startup table leaked an API key:\n{table}"
        );
    }

    /// Without a bound, one hung builder wedges the exit — a regression from the
    /// straight-line return this replaces. Paused time makes the wait instant.
    #[tokio::test(start_paused = true)]
    async fn the_shutdown_join_is_bounded_and_names_stragglers() {
        let quick = tokio::spawn(async {});
        let stuck = tokio::spawn(std::future::pending::<()>());
        let began = tokio::time::Instant::now();
        let stragglers = join_tasks(vec![
            ("quick".to_owned(), quick),
            ("stuck".to_owned(), stuck),
        ])
        .await;
        assert_eq!(stragglers, vec!["stuck".to_owned()]);
        assert!(began.elapsed() <= SHUTDOWN_GRACE);
    }

    /// Shutdown withdraws at every builder holding a live quote.
    #[tokio::test]
    async fn shutdown_withdraws_the_live_quote_at_each_builder() {
        let a = mock::spawn(mock::Behaviour::Ack).await;
        let b = mock::spawn(mock::Behaviour::Ack).await;
        let h = Harness::spawn(&[("a", &a.url), ("b", &b.url)], Duration::from_millis(20));
        h.cmd
            .send(Command::Quote(quote(17, vec![0x02, 0x0a])))
            .unwrap();
        assert!(a.wait_for(1, Duration::from_secs(2)).await);
        assert!(b.wait_for(1, Duration::from_secs(2)).await);

        let before = (a.count(), b.count());
        h.shutdown().await;
        assert!(
            a.count() > before.0 && b.count() > before.1,
            "no cancel was sent"
        );
        assert!(a.received().last().unwrap().tx.is_empty());
        assert!(b.received().last().unwrap().tx.is_empty());
    }

    /// The line an operator reads once per block: one column per builder, in
    /// configuration order so the columns never shuffle between blocks.
    #[test]
    fn the_summary_reports_every_builder_in_configuration_order() {
        let mut summary = BlockSummary::new(21_500_431, &names());
        summary.record(&event(
            "quasar",
            Some(21_500_431),
            EventKind::Acked { latency_ms: 31 },
        ));
        summary.record(&event(
            "titan",
            Some(21_500_431),
            EventKind::Acked { latency_ms: 8 },
        ));
        summary.record(&event(
            "buildernet",
            None,
            EventKind::Disconnected {
                error: "connect timed out".to_owned(),
            },
        ));

        let rendered = summary.render(&Quoted::Slots {
            delta: U256::from(500_000_000_000_000u64),
            mid: U256::exp10(18),
            ts: 1_755_000_012,
        });
        assert!(rendered.contains("quoting block 21500431"), "{rendered}");
        assert!(rendered.contains("at timestamp 1755000012"), "{rendered}");
        // Configuration order, not arrival order: quasar acked first but titan is listed
        // first, so a glance down the column always means the same builder.
        let line = rendered.lines().last().unwrap();
        let titan = line.find("titan").unwrap();
        let bnet = line.find("buildernet").unwrap();
        let quasar = line.find("quasar").unwrap();
        assert!(titan < bnet && bnet < quasar, "columns shuffled: {line}");
        assert!(line.contains("titan ack 8ms"), "{line}");
        assert!(line.contains("quasar ack 31ms"), "{line}");
        assert!(line.contains("buildernet down"), "{line}");
        assert_eq!(summary.live(), 2);
    }

    /// A block with no usable price still prints. Silence there would be
    /// indistinguishable from a stalled service.
    #[test]
    fn a_withdrawn_block_still_prints_and_says_why() {
        let mut summary = BlockSummary::new(21_500_434, &names());
        for name in ["titan", "quasar"] {
            summary.record(&event(name, Some(21_500_434), EventKind::Withdrawn));
        }
        summary.record(&event(
            "buildernet",
            None,
            EventKind::Disconnected {
                error: "connect timed out".to_owned(),
            },
        ));

        let rendered = summary.render(&Quoted::Withdrawn(
            "latest feed price is 41s old".to_owned(),
        ));
        assert!(
            rendered.contains("quoting block 21500434: withdrawn"),
            "{rendered}"
        );
        assert!(
            rendered.contains("latest feed price is 41s old"),
            "{rendered}"
        );
        let line = rendered.lines().last().unwrap();
        assert!(line.contains("titan withdrawn"), "{line}");
        assert!(line.contains("buildernet down"), "{line}");
    }

    /// A rejection is not a disconnect: the builder is reachable and said no, which is a
    /// different thing to chase, so it must not be flattened into "down".
    #[test]
    fn a_rejection_is_reported_distinctly_from_a_disconnect() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event(
            "titan",
            Some(9),
            EventKind::Rejected {
                error: "quote too old".to_owned(),
            },
        ));
        let line = summary
            .render(&Quoted::Slots {
                delta: U256::zero(),
                mid: U256::one(),
                ts: 1,
            })
            .lines()
            .last()
            .unwrap()
            .to_owned();
        assert!(line.contains("titan rejected: quote too old"), "{line}");
        assert!(!line.contains("titan down"), "{line}");
    }

    /// A reconnect during the block restores the builder's column, so a recovery is
    /// visible on the next line rather than only in a one-off message.
    #[test]
    fn a_reconnect_clears_a_builders_down_state() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event(
            "buildernet",
            None,
            EventKind::Disconnected {
                error: "connect timed out".to_owned(),
            },
        ));
        assert_eq!(summary.live(), 2);
        summary.record(&event("buildernet", None, EventKind::Reconnected));
        assert_eq!(summary.live(), 3);
    }

    /// A reconnect is the one transition an operator cannot see from the per-block columns
    /// alone — a disconnect shows up as the very next line reading "down", but a recovery
    /// just goes quiet — so `fold` hands back a standalone line for it, counting how many
    /// of the configured builders are live after this event.
    #[test]
    fn folding_a_reconnect_reports_one_line_with_the_live_count() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event(
            "buildernet",
            None,
            EventKind::Disconnected {
                error: "connect timed out".to_owned(),
            },
        ));
        assert_eq!(summary.live(), 2);

        let line = summary
            .fold(&event("buildernet", None, EventKind::Reconnected))
            .expect("a reconnect must produce a line");
        assert_eq!(line, "builder buildernet reconnected; 3/3 live");
        assert_eq!(summary.live(), 3, "fold must still record the event");
    }

    /// Every other event kind folds in exactly as `record` alone would, but reports
    /// nothing: only a reconnect gets a standalone line.
    #[test]
    fn folding_a_non_reconnect_event_reports_nothing() {
        let mut summary = BlockSummary::new(9, &names());
        let line = summary.fold(&event("titan", Some(9), EventKind::Acked { latency_ms: 4 }));
        assert!(line.is_none(), "{line:?}");
        let rendered = summary.render(&Quoted::Withdrawn("no price yet".to_owned()));
        assert!(rendered.contains("titan ack 4ms"), "{rendered}");
    }

    /// Why a builder is down only ever reaches an operator through this line: a task
    /// reports a lost connection once per distinct error and nothing else consumes that
    /// report, so a bare "down" would leave nothing to distinguish a refused key from a
    /// dead socket.
    #[test]
    fn a_down_builder_reports_why_it_is_down() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event(
            "buildernet",
            None,
            EventKind::Disconnected {
                error: "401 unauthorized".to_owned(),
            },
        ));
        let rendered = summary.render(&Quoted::Withdrawn("no price yet".to_owned()));
        let line = rendered.lines().last().unwrap();
        assert!(line.contains("buildernet down: 401 unauthorized"), "{line}");
    }

    /// A per-block answer for a block that already passed must not land in this block's
    /// line: a task's ack can arrive after the loop rolled to the next target, and folding
    /// it in would show that column as having answered for a block it has not been asked
    /// about yet. A disconnect is the exception — it names the block it was noticed in, but
    /// being down is connection state that outlives it.
    #[test]
    fn a_stale_per_block_event_is_ignored_but_a_stale_disconnect_is_not() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event("titan", Some(8), EventKind::Acked { latency_ms: 4 }));
        let rendered = summary.render(&Quoted::Withdrawn("no price yet".to_owned()));
        let line = rendered.lines().last().unwrap();
        assert!(
            line.contains("titan no ack"),
            "stale ack was folded in: {line}"
        );

        summary.record(&event(
            "titan",
            Some(8),
            EventKind::Disconnected {
                error: "broken pipe".to_owned(),
            },
        ));
        let rendered = summary.render(&Quoted::Withdrawn("no price yet".to_owned()));
        let line = rendered.lines().last().unwrap();
        assert!(line.contains("titan down: broken pipe"), "{line}");
        assert_eq!(summary.live(), 2);
    }

    /// Events naming a builder that is not configured cannot happen, but folding one in
    /// must not panic a running service.
    #[test]
    fn an_unknown_builder_name_is_ignored() {
        let mut summary = BlockSummary::new(9, &names());
        summary.record(&event("ghost", Some(9), EventKind::Acked { latency_ms: 1 }));
        assert_eq!(summary.live(), 3);
    }

    use eyre::eyre;

    use crate::breaker::BreakerConfig;
    use crate::guard::Latch;

    /// The halt's bounded wait keeps going while any builder might still report its
    /// cancel. A builder that is down cannot and one that has reported withdrawn need not;
    /// everyone else might — including a builder merely *pending* because `carry_over`
    /// reset it at the block boundary while it is still active from the block before. The
    /// wait used to be gated on an ack *this* block, which is false on a first-tick trip,
    /// the common case, so the cancel acks were never collected or shown.
    #[test]
    fn a_halt_waits_for_every_builder_that_could_still_report_its_cancel() {
        let names: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let mut summary = BlockSummary::new(9, &names);
        assert!(
            summary.awaiting_cancels(),
            "pending builders may still be active from the last block"
        );
        summary.record(&event("a", Some(9), EventKind::Withdrawn));
        summary.record(&event("b", Some(9), EventKind::Acked { latency_ms: 1 }));
        assert!(
            summary.awaiting_cancels(),
            "b acked this block and c is pending"
        );
        summary.record(&event("b", Some(9), EventKind::Withdrawn));
        summary.record(&event(
            "c",
            None,
            EventKind::Disconnected {
                error: "gone".to_owned(),
            },
        ));
        assert!(
            !summary.awaiting_cancels(),
            "withdrawn and down builders have nothing more to say"
        );
    }

    /// A JSON-RPC stub for the per-block reads `drive` makes, served over a raw HTTP/1.1
    /// loop so the test goes through the real `EthClient`. One fixed parent block,
    /// forever; every method called is recorded so a test can assert what `drive` did
    /// *not* ask for. `eth_blockNumber` answers one past the fixed parent — realistic (a
    /// live chain's head keeps moving) and it is what lets a test drive the head-catch-up
    /// path deterministically, on the very first poll, without a stateful stub.
    struct ChainStub {
        url: String,
        calls: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ChainStub {
        async fn spawn(parent_number: u64) -> Self {
            use ethrex_common::types::BlockHeader;
            use ethrex_rpc::types::block::{BlockBodyWrapper, OnlyHashesBlockBody, RpcBlock};

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorder = calls.clone();
            // A real block, serialized the way a node would: `UpdateStamp::from_head`
            // needs a timestamp and a base fee, and the client's deserializer needs
            // every header field present.
            //
            // Stamped now, not at a fixed past instant: `drive` arms a missed-slot timer at
            // the parent's timestamp plus one block time, so a stub claiming "latest" is a
            // block from 2023 has that timer fire on the first pass and re-sign the quote —
            // a second publish, in a test counting the first.
            let block = serde_json::to_value(RpcBlock {
                hash: ethrex_common::H256::zero(),
                size: 0,
                header: BlockHeader {
                    number: parent_number,
                    timestamp: wall_clock_secs(),
                    gas_limit: 30_000_000,
                    base_fee_per_gas: Some(1_000_000_000),
                    ..Default::default()
                },
                body: BlockBodyWrapper::OnlyHashes(OnlyHashesBlockBody {
                    transactions: vec![],
                    uncles: vec![],
                    withdrawals: vec![],
                }),
            })
            .unwrap();
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    let recorder = recorder.clone();
                    let block = block.clone();
                    tokio::spawn(async move {
                        let mut buffered = Vec::new();
                        while let Some(request) = read_http_json(&mut stream, &mut buffered).await {
                            let method = request["method"].as_str().unwrap_or("").to_owned();
                            recorder.lock().unwrap().push(method.clone());
                            let result = match method.as_str() {
                                "eth_getBlockByNumber" => block.clone(),
                                "eth_getTransactionCount" => serde_json::json!("0x0"),
                                "eth_getBalance" => serde_json::json!("0xde0b6b3a7640000"),
                                "eth_blockNumber" => {
                                    serde_json::json!(format!("{:#x}", parent_number + 1))
                                }
                                _ => serde_json::json!("0x"),
                            };
                            let body = serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": request["id"].clone(),
                                "result": result,
                            })
                            .to_string();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                                 content-length: {}\r\n\r\n{body}",
                                body.len()
                            );
                            if tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes())
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    });
                }
            });
            Self {
                url: format!("http://{addr}"),
                calls,
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    /// Reads one HTTP request from `stream` (keep-alive: `buffered` carries anything read
    /// past the end of it) and returns its JSON body; `None` once the peer is gone.
    async fn read_http_json(
        stream: &mut tokio::net::TcpStream,
        buffered: &mut Vec<u8>,
    ) -> Option<serde_json::Value> {
        loop {
            if let Some(end) = buffered.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buffered[..end]).into_owned();
                let length: usize = head
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.trim().parse().ok())
                    .unwrap_or(0);
                let body = end + 4;
                if buffered.len() >= body + length {
                    let json = serde_json::from_slice(&buffered[body..body + length]).ok();
                    buffered.drain(..body + length);
                    return json;
                }
            }
            let mut chunk = [0u8; 4096];
            match tokio::io::AsyncReadExt::read(stream, &mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => buffered.extend_from_slice(&chunk[..n]),
            }
        }
    }

    /// A fresh feed sample at `mid` (whole units, 6 decimals), stamped now. Distinct from
    /// `sample` above, which takes an already-scaled mid at 18 decimals.
    fn sample_6dp(mid: u64) -> crate::feed::PriceSample {
        crate::feed::PriceSample {
            delta: U256::from(500),
            mid: scaled(mid).1,
            at: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }
    }

    /// The halt's exit sequence, end to end through `drive` against a stubbed chain and
    /// mock builders — the stretch three rounds of review found defects in and nothing
    /// exercised. On a trip the loop withdraws at every builder holding the quote, for
    /// the block it was quoting; waits for their cancel acks; ends the pair with
    /// `Ended::Halted`; and never runs the landing check for a block it left early.
    #[tokio::test]
    async fn a_trip_withdraws_at_every_builder_and_halts_the_pair() {
        use crate::{
            config::{MidBand, Pair},
            feed::PriceSample,
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        let mock_a = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mock_b = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mut harness = Harness::spawn(
            &[("a", &mock_a.url), ("b", &mock_b.url)],
            Duration::from_millis(20),
        );

        // The test plays the feed task: judge, then publish, the way `read_stream` does.
        let breaker = two_percent();
        let (feed, rx): (watch::Sender<Option<PriceSample>>, _) = watch::channel(None);
        judge(&breaker, &[100], 0);
        feed.send_replace(Some(sample_6dp(100)));

        let secret = secp256k1::SecretKey::from_slice(&[0x11; 32]).unwrap();
        let lane = U256::from(7);
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: None,
                    max: None,
                },
            },
            values: ValueSource::feed_for_tests(rx, None, false, 6),
            latch: latch_of(&breaker),
            armed: true,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: test_pair_metrics(),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            once: false,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names = names();
        // The head watcher polls the stub, which stays at block 99 forever, so the target
        // block never passes and only the trip can end the loop.
        let mut head_tasks = crate::tasks::Tasks::default();
        let mut head = head::spawn(
            &mut head_tasks,
            client.clone(),
            Duration::from_millis(20),
            head::POLL_TIMEOUT,
            None,
            head_metrics(),
        );

        // Once both builders hold the quote, the feed moves 3%.
        let jump = {
            let (a, b, breaker) = (mock_a.clone(), mock_b.clone(), breaker.clone());
            tokio::spawn(async move {
                let held = Duration::from_secs(5);
                assert!(a.wait_for(1, held).await, "a never received the quote");
                assert!(b.wait_for(1, held).await, "b never received the quote");
                judge(&breaker, &[103], 0);
                feed.send_replace(Some(sample_6dp(103)));
            })
        };

        let ended = tokio::time::timeout(
            Duration::from_secs(10),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                shutdown_channel().1,
            ),
        )
        .await
        .expect("drive must return on the trip instead of quoting on");
        assert_eq!(ended.unwrap(), Ended::Halted);

        // `drive`'s own future has already returned above, so every recording site it
        // could have hit this run has already run too — no intervening await needed for
        // these to be settled. The 100 price published exactly once (a same-price tick
        // decides Nothing, not Publish again) and the trip halts on its first observation
        // (the loop breaks immediately), so both counts are exact, not just non-zero.
        assert_eq!(
            live.metrics.publish_decisions(PublishAction::Publish).get(),
            1,
            "the gate must record the one tick that actually published"
        );
        assert_eq!(
            live.metrics.publish_decisions(PublishAction::Halt).get(),
            1,
            "the gate must record the trip that ended the pair"
        );
        // scaled(100) is (delta=1, mid=100e6) at 6 decimals: mid renders as a whole 100.0,
        // and delta only needs to have left its zero-value default to prove the gauge was
        // touched at all.
        assert_eq!(
            live.metrics.published_mid.get(),
            100.0,
            "published_mid must reflect what was actually signed and sent"
        );
        assert!(
            live.metrics.published_delta.get() > 0.0,
            "published_delta must leave its zero default once a quote is signed and sent"
        );
        // `blocks_seen` starts at 0 and 0 is a multiple of every `RUNWAY_CHECK_BLOCKS`, so
        // the runway check always runs on a lane's first block — this halted-after-one-block
        // run still exercises it exactly once.
        assert_eq!(
            live.metrics.target_blocks.get(),
            1,
            "one outer-loop pass ran before the halt"
        );
        // ChainStub answers eth_getBalance with 0xde0b6b3a7640000 (1 ETH) and the parent
        // block carries a 1 gwei base fee; UPDATE_GAS_LIMIT (120_000) makes the exact runway
        // 1e18 / (120_000 * 1e9) = 8333 updates. Exact, not just non-zero, because both
        // inputs are pinned by the stub.
        // The page-severity PusherSignerNearlyDry alert reads this series directly — it is a
        // predict_linear over the balance — so a wiring mistake here would go unnoticed until
        // it misfired on a key that was actually fine.
        assert_eq!(
            live.metrics.signer_balance_wei.get(),
            1_000_000_000_000_000_000.0,
            "signer_balance_wei must reflect the stub's eth_getBalance answer"
        );
        // No alert rule reads this one any more; the "Runway" panel does, and it is what an
        // operator reaches for once the page above has woken them, so a wrong number here is
        // still a wrong answer to the only question they will be asking.
        assert_eq!(
            live.metrics.signer_runway_updates.get(),
            8333.0,
            "signer_runway_updates must be set from preflight::runway_row"
        );
        // No assertion on GetBlockByNumber or GetBlockNumber: both left this loop for the
        // process-wide watcher in head.rs, which serves every pair and so has no pair to
        // label a sample with. Their `call` children are consequently never recorded — see
        // the note on `RpcCall` — which is why this test pins the two that remain instead of
        // being deleted along with them.
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetNonce)
                .get_sample_count(),
            1,
            "the nonce fetch must be timed"
        );
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetBalance)
                .get_sample_count(),
            1,
            "the runway balance read must be timed"
        );

        jump.await.unwrap();

        // The cancel reached every builder holding the quote, for the block it was quoting
        // (the parent is 99, so the target was 100).
        for endpoint in [&mock_a, &mock_b] {
            assert!(
                wait_for_withdraw(endpoint, Duration::from_secs(2)).await,
                "no cancel reached {}",
                endpoint.url
            );
            let cancel = endpoint
                .received()
                .into_iter()
                .find(|update| update.tx.is_empty())
                .unwrap();
            assert_eq!(cancel.block_number, 100);
        }

        // No landing check for a block the loop left early: nothing read the lane back.
        // (eth_blockNumber is the head watcher's poll, which runs regardless.)
        let calls = chain.calls();
        assert!(
            calls.iter().any(|method| method == "eth_getBlockByNumber"),
            "the stub was never asked for a block: {calls:?}"
        );
        assert!(
            !calls.iter().any(|method| method == "eth_call"),
            "a halted block must not be verified: {calls:?}"
        );
        harness.shutdown().await;
    }

    /// A trip that lands while the loop is waiting (on the head, with no price move
    /// pending: what a restart backoff or an RPC outage looks like from the latch) is acted
    /// on at once. The loop wakes on the latch, withdraws at every builder, and halts with
    /// the reason's alarm, the same path a mid-tick trip takes.
    #[tokio::test]
    async fn a_trip_that_happened_while_the_loop_was_away_halts_it_on_return() {
        use crate::{
            config::{MidBand, Pair},
            feed::PriceSample,
            guard::{Cause, TripReason},
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        // What the loop said, for the alarm line: a current-thread runtime, so every task
        // of this test logs through this thread's default subscriber.
        let (said, subscriber) = crate::output::capturing();
        let _said = tracing::subscriber::set_default(subscriber);

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        let mock_a = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mock_b = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mut harness = Harness::spawn(
            &[("a", &mock_a.url), ("b", &mock_b.url)],
            Duration::from_millis(20),
        );

        let latch = Latch::new();
        // The lane's series, attached as `build_live` does: the latch records the trip.
        let metrics = test_pair_metrics();
        let adopted = crate::feed::Adoption::new();
        adopted.adopt();
        latch.attach_metrics(crate::feed::Gated::new(metrics.clone(), adopted));
        let (feed, rx): (watch::Sender<Option<PriceSample>>, _) = watch::channel(None);
        feed.send_replace(Some(sample_6dp(100)));

        let secret = secp256k1::SecretKey::from_slice(&[0x11; 32]).unwrap();
        let lane = U256::from(7);
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: None,
                    max: None,
                },
            },
            values: ValueSource::feed_for_tests(rx, None, false, 6),
            latch: latch.clone(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics,
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            once: false,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names = names();
        let mut head_tasks = crate::tasks::Tasks::default();
        let mut head = head::spawn(
            &mut head_tasks,
            client.clone(),
            Duration::from_millis(20),
            head::POLL_TIMEOUT,
            None,
            head_metrics(),
        );

        // Once both builders hold the quote, something outside the loop trips the lane:
        // no price moves, no head arrives, only the latch.
        let trip = {
            let (a, b, latch) = (mock_a.clone(), mock_b.clone(), latch.clone());
            tokio::spawn(async move {
                let held = Duration::from_secs(5);
                assert!(a.wait_for(1, held).await, "a never received the quote");
                assert!(b.wait_for(1, held).await, "b never received the quote");
                assert!(latch.trip(
                    "kill",
                    Cause::External,
                    TripReason::new("operator kill switch")
                ));
            })
        };

        let ended = tokio::time::timeout(
            Duration::from_secs(10),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                shutdown_channel().1,
            ),
        )
        .await
        .expect("drive must wake on the latch instead of waiting for a price or a head");
        assert_eq!(ended.unwrap(), Ended::Halted);
        trip.await.unwrap();
        assert_eq!(
            live.metrics.publish_decisions(PublishAction::Halt).get(),
            1,
            "the gate recorded the halt"
        );
        // A trip from outside the composite (a component's task, a kill of this kind) is
        // recorded by the latch itself, once, whichever task woke first.
        assert_eq!(live.metrics.breaker_tripped.get(), 1.0);
        assert_eq!(live.metrics.breaker_trips.get(), 1);
        assert_eq!(live.metrics.trip_source("kill", "external").get(), 1.0);
        // The alarm, once, with the reason's text: the same line a mid-tick trip prints.
        // (The block's summary header repeats the text as the withdraw reason; not it.)
        let lines = said.lines();
        let alarm_lines: Vec<&String> = lines
            .iter()
            .filter(|(_, line)| line.contains("kill switch") && !line.contains("withdrawn ("))
            .map(|(_, line)| line)
            .collect();
        assert_eq!(alarm_lines.len(), 1, "one alarm line: {lines:?}");
        for endpoint in [&mock_a, &mock_b] {
            assert!(
                wait_for_withdraw(endpoint, Duration::from_secs(2)).await,
                "no cancel reached {}",
                endpoint.url
            );
        }
        harness.shutdown().await;
    }

    /// The counterpart above proves the tick check reaches the halt path; `ten_percent_window`
    /// disables the tick check entirely, so this proves the window check reaches the very
    /// same path on its own — the halt under test here is unambiguously the window's.
    #[tokio::test]
    async fn a_window_trip_withdraws_at_every_builder_and_halts_the_pair() {
        use crate::{
            config::{MidBand, Pair},
            feed::PriceSample,
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        let mock_a = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mock_b = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mut harness = Harness::spawn(
            &[("a", &mock_a.url), ("b", &mock_b.url)],
            Duration::from_millis(20),
        );

        // The test plays the feed task: judge, then publish, the way `read_stream` does.
        let breaker = ten_percent_window();
        let (feed, rx): (watch::Sender<Option<PriceSample>>, _) = watch::channel(None);
        judge(&breaker, &[100], 0);
        feed.send_replace(Some(sample_6dp(100)));

        let secret = secp256k1::SecretKey::from_slice(&[0x11; 32]).unwrap();
        let lane = U256::from(7);
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: None,
                    max: None,
                },
            },
            values: ValueSource::feed_for_tests(rx, None, false, 6),
            latch: latch_of(&breaker),
            armed: true,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: test_pair_metrics(),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            once: false,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names = names();
        // The head watcher polls the stub, which stays at block 99 forever, so the target
        // block never passes and only the trip can end the loop.
        let mut head_tasks = crate::tasks::Tasks::default();
        let mut head = head::spawn(
            &mut head_tasks,
            client.clone(),
            Duration::from_millis(20),
            head::POLL_TIMEOUT,
            None,
            head_metrics(),
        );

        // Once both builders hold the quote, the feed moves 11% — a second later, so the
        // window buffer records a distinct bucket rather than collapsing into the first.
        let jump = {
            let (a, b, breaker) = (mock_a.clone(), mock_b.clone(), breaker.clone());
            tokio::spawn(async move {
                let held = Duration::from_secs(5);
                assert!(a.wait_for(1, held).await, "a never received the quote");
                assert!(b.wait_for(1, held).await, "b never received the quote");
                judge(&breaker, &[111], 1);
                feed.send_replace(Some(sample_6dp(111)));
            })
        };

        let ended = tokio::time::timeout(
            Duration::from_secs(10),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                shutdown_channel().1,
            ),
        )
        .await
        .expect("drive must return on the trip instead of quoting on");
        assert_eq!(ended.unwrap(), Ended::Halted);

        // The trip's own text names the window size and the anchor label — the two
        // pieces `halt` threads through `span` and `reference_label` for a windowed
        // trip. Nothing else in this test touches either string, so a format string
        // that dropped one would still pass every assertion above. `drive` owns its
        // gate internally, but the lane's latch it tripped is still latched here, so a
        // fresh gate over the same latch reaches `halt` with the identical reason.
        let mut probe = PublishGate::new(latch_of(&breaker));
        let trip = probe.decide(Ok(scaled(111)));
        assert_eq!(trip.action, Action::Halt);
        assert!(
            trip.announce
                .is_some_and(|line| line.contains("over 5 blocks") && line.contains("anchor")),
            "the announce line must name the window size and the anchor label"
        );
        assert!(
            probe
                .withdrawn_reason()
                .is_some_and(|r| r.contains("over 5 blocks")),
            "the withdrawn reason must name the window size"
        );

        // `drive`'s own future has already returned above, so every recording site it
        // could have hit this run has already run too — no intervening await needed for
        // these to be settled. The 100 price published exactly once (a same-price tick
        // decides Nothing, not Publish again) and the trip halts on its first observation
        // (the loop breaks immediately), so both counts are exact, not just non-zero.
        assert_eq!(
            live.metrics.publish_decisions(PublishAction::Publish).get(),
            1,
            "the gate must record the one tick that actually published"
        );
        assert_eq!(
            live.metrics.publish_decisions(PublishAction::Halt).get(),
            1,
            "the gate must record the trip that ended the pair"
        );
        // scaled(100) is (delta=1, mid=100e6) at 6 decimals: mid renders as a whole 100.0,
        // and delta only needs to have left its zero-value default to prove the gauge was
        // touched at all.
        assert_eq!(
            live.metrics.published_mid.get(),
            100.0,
            "published_mid must reflect what was actually signed and sent"
        );
        assert!(
            live.metrics.published_delta.get() > 0.0,
            "published_delta must leave its zero default once a quote is signed and sent"
        );
        // `blocks_seen` starts at 0 and 0 is a multiple of every `RUNWAY_CHECK_BLOCKS`, so
        // the runway check always runs on a lane's first block — this halted-after-one-block
        // run still exercises it exactly once.
        assert_eq!(
            live.metrics.target_blocks.get(),
            1,
            "one outer-loop pass ran before the halt"
        );
        // ChainStub answers eth_getBalance with 0xde0b6b3a7640000 (1 ETH) and the parent
        // block carries a 1 gwei base fee; UPDATE_GAS_LIMIT (120_000) makes the exact runway
        // 1e18 / (120_000 * 1e9) = 8333 updates. Exact, not just non-zero, because both
        // inputs are pinned by the stub.
        // The page-severity PusherSignerNearlyDry alert reads this series directly — it is a
        // predict_linear over the balance — so a wiring mistake here would go unnoticed until
        // it misfired on a key that was actually fine.
        assert_eq!(
            live.metrics.signer_balance_wei.get(),
            1_000_000_000_000_000_000.0,
            "signer_balance_wei must reflect the stub's eth_getBalance answer"
        );
        // No alert rule reads this one any more; the "Runway" panel does, and it is what an
        // operator reaches for once the page above has woken them, so a wrong number here is
        // still a wrong answer to the only question they will be asking.
        assert_eq!(
            live.metrics.signer_runway_updates.get(),
            8333.0,
            "signer_runway_updates must be set from preflight::runway_row"
        );
        // No assertion on GetBlockByNumber or GetBlockNumber: both left this loop for the
        // process-wide watcher in head.rs, which serves every pair and so has no pair to
        // label a sample with. Their `call` children are consequently never recorded — see
        // the note on `RpcCall` — which is why this test pins the two that remain instead of
        // being deleted along with them.
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetNonce)
                .get_sample_count(),
            1,
            "the nonce fetch must be timed"
        );
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetBalance)
                .get_sample_count(),
            1,
            "the runway balance read must be timed"
        );

        jump.await.unwrap();

        // The cancel reached every builder holding the quote, for the block it was quoting
        // (the parent is 99, so the target was 100).
        for endpoint in [&mock_a, &mock_b] {
            assert!(
                wait_for_withdraw(endpoint, Duration::from_secs(2)).await,
                "no cancel reached {}",
                endpoint.url
            );
            let cancel = endpoint
                .received()
                .into_iter()
                .find(|update| update.tx.is_empty())
                .unwrap();
            assert_eq!(cancel.block_number, 100);
        }

        // No landing check for a block the loop left early: nothing read the lane back.
        // (eth_blockNumber is the head watcher's poll, which runs regardless.)
        let calls = chain.calls();
        assert!(
            calls.iter().any(|method| method == "eth_getBlockByNumber"),
            "the stub was never asked for a block: {calls:?}"
        );
        assert!(
            !calls.iter().any(|method| method == "eth_call"),
            "a halted block must not be verified: {calls:?}"
        );
        harness.shutdown().await;
    }

    /// `price_unusable` is the one recording site with no coverage elsewhere: it stands
    /// between a corrupt book and an arbitrary price reaching the chain, since `OutOfBand`
    /// is what the `PusherMidOutOfBand` alert (ticket severity — the withdrawal itself
    /// already stopped a bad price from landing) fires on. A `Static` source
    /// pinned outside its own `MidBand` gives a deterministic, zero-timing-dependency way
    /// to hit it through the real `drive` path, with no feed, breaker or builder needed.
    /// `config.rs` would refuse this combination at parse time — a static mid is validated
    /// against its own band before the process ever starts — but a test building `Live`
    /// directly bypasses that guard, which is exactly what makes the path reachable here.
    #[tokio::test]
    async fn an_out_of_band_static_mid_is_recorded_before_the_gate_converts_it() {
        use crate::{
            config::{MidBand, Pair},
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        // No builders: the price never becomes usable, so nothing is ever signed or sent,
        // and the wire is not what this test is about.
        let mut harness = Harness::spawn(&[], Duration::from_millis(20));

        let secret = secp256k1::SecretKey::from_slice(&[0x22; 32]).unwrap();
        let lane = U256::from(7);
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: Some(U256::exp10(18)),
                    max: None,
                },
            },
            values: ValueSource::fixed_for_tests(U256::one(), U256::exp10(17), 6),
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: test_pair_metrics(),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            once: false,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names: Vec<String> = Vec::new();

        let (stop, cancel) = shutdown_channel();
        // The head `drive` follows, held at 99 for the life of the test: the target block
        // never passes, so the only thing that can end the loop is the shutdown below.
        // Stamped from the current wall clock, like the stub's own block, so the target's
        // slot is genuinely ahead and the missed-slot re-stamp cannot fire inside the test.
        let (_heads, mut head) = watch::channel(Head {
            number: 99,
            timestamp: wall_clock_secs(),
            base_fee_per_gas: Some(1_000_000_000),
        });
        // A `Static` source never fires `changed()` (see `ValueSource::changed`), so the
        // loop's first pass — where the out-of-band price is read and recorded — runs to
        // completion synchronously and the loop then parks in `select!` waiting for
        // exactly this signal: with the head pinned, no other arm can become ready and
        // race a second pass in before shutdown lands.
        let stopper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = stop.send(true);
        });

        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                cancel,
            ),
        )
        .await
        .expect("drive must return once shutdown is requested");
        assert_eq!(ended.unwrap(), Ended::Finished);
        stopper.await.unwrap();

        // `drive`'s own future has already returned above, so the one recording site this
        // test exists for has already run — no intervening await needed for this to be
        // settled.
        assert_eq!(
            live.metrics
                .price_unusable(crate::update::UnusableKind::OutOfBand)
                .get(),
            1,
            "an out-of-band tick must be recorded with its kind before decide() converts \
             the error and the kind is lost"
        );

        harness.shutdown().await;
    }

    /// The landing-verification tail of `drive`, end to end: ChainStub answers
    /// `eth_blockNumber` one block ahead of its fixed parent, so the head-catch-up poll
    /// succeeds on its very first tick and the target block "passes" instead of the loop
    /// halting or being cancelled first — the two shapes the other `drive` tests above cover.
    /// With a builder holding the quote, that exercises the whole tail: `verify_landed`'s
    /// `eth_call` (tracked under `RpcCall::EthCall`), the landing outcome it produces, and
    /// the tracker's miss count. ChainStub has no case for `eth_call`, so it
    /// falls to the same "0x" default as any other unmatched method: a read that succeeds at
    /// the transport but decodes to nothing, which is a `Landing::Unknown` — the same bucket
    /// a genuinely unreadable lane lands in, and the one `PusherLandingUnverified` counts.
    #[tokio::test]
    async fn a_passed_block_records_its_landing_and_the_ethcall_that_checked_it() {
        use crate::{
            config::{MidBand, Pair},
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        // Arc'd so the task that advances the head below can hold it too, the same way the
        // breaker test shares its mocks with the task that moves the feed.
        let mock_a = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mut harness = Harness::spawn(&[("a", &mock_a.url)], Duration::from_millis(20));

        let secret = secp256k1::SecretKey::from_slice(&[0x33; 32]).unwrap();
        let lane = U256::from(7);
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: None,
                    max: None,
                },
            },
            // `Static`, like the out-of-band test above, so the price is available on the
            // very first pass with no feed task and no timing dependency of its own.
            values: ValueSource::fixed_for_tests(U256::one(), scaled(100).1, 6),
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: test_pair_metrics(),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            // Ends `drive` right after the one block cycle this test is about, instead of
            // requiring a second shutdown mechanism to race against it.
            once: true,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names = vec!["a".to_owned()];

        // The head is driven by hand rather than by `head::spawn`: the block has to pass at
        // a moment this test controls, and a watcher would only ever report what the stub
        // says. Stamped from the current wall clock, like the stub's own block, so the
        // target's slot is ahead and the missed-slot re-stamp cannot fire mid-test.
        let head_at = |number: u64| Head {
            number,
            timestamp: wall_clock_secs(),
            base_fee_per_gas: Some(1_000_000_000),
        };
        let (heads, mut head) = watch::channel(head_at(99));
        // Block 100 lands once the builder is holding the quote for it. Keyed on the
        // builder's receipt rather than a bare sleep, then given a margin: the ack that
        // follows the receipt is what makes `any_acked()` true, and `any_acked()` is the
        // condition for the landing check this test exists to cover. Publishing the new
        // head the instant the mock received the quote would put the head arm and the
        // ack event in the same `select!` pass, where the head could win and the block
        // would pass as `NotQuoted`. 100ms is ~1000x a loopback ack, and is the same margin
        // the old head-poll ticker gave this test implicitly.
        let landed = {
            let mock_a = mock_a.clone();
            tokio::spawn(async move {
                assert!(
                    mock_a.wait_for(1, Duration::from_secs(5)).await,
                    "the builder never received the quote"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
                heads.send_replace(head_at(100));
            })
        };

        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                shutdown_channel().1,
            ),
        )
        .await
        .expect("drive must return once the target block has passed");
        assert_eq!(ended.unwrap(), Ended::Finished);
        landed.await.unwrap();

        // `drive`'s own future has already returned above, so every recording site the
        // block-passed tail could have hit has already run — no intervening await needed
        // for these to be settled.
        assert_eq!(
            live.metrics.target_blocks.get(),
            1,
            "one outer-loop pass ran"
        );
        // Not the head poll: that moved to the process-wide watcher in head.rs, which has
        // no pair to label a sample with. What `drive` still calls per pair is
        // `fetch_block_inputs`, once here — `--once` skips the next block's fetch — and
        // both of its calls must be timed separately, since they now overlap.
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetNonce)
                .get_sample_count(),
            1,
            "the nonce fetch must be timed"
        );
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::GetBalance)
                .get_sample_count(),
            1,
            "the runway check's balance read must be timed on its own, not folded into \
             the nonce's sample"
        );
        assert_eq!(
            live.metrics
                .rpc_duration(RpcCall::EthCall)
                .get_sample_count(),
            1,
            "verify_landed's read-back must be timed"
        );
        assert_eq!(
            live.metrics.rpc_errors(RpcCall::EthCall).get(),
            0,
            "the stub's eth_call answers 200 OK; the miss below is a decode failure, not a \
             transport error"
        );
        assert_eq!(
            live.metrics.landings(LandingResult::Unknown).get(),
            1,
            "an unverifiable read-back must be recorded as Unknown, the bucket \
             PusherLandingUnverified counts"
        );
        assert_eq!(
            live.metrics.consecutive_landing_misses.get(),
            1.0,
            "the tracker's first miss must reach the gauge"
        );

        harness.shutdown().await;
    }

    /// The same block cycle, read from the pricer's side: the feed a build context hands
    /// out (`BuildCtx::landings`) carries the block's outcome once the read-back has it,
    /// so a strategy adapts to what happened to its own quotes. Empty until then.
    #[tokio::test]
    async fn a_pricer_sees_each_blocks_landing() {
        use crate::{
            config::{MidBand, Pair},
            pricing::{BuildCtx, LandingOutcome},
            supervisor::shutdown_channel,
            update::ValueSource,
        };
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let chain = ChainStub::spawn(99).await;
        let client = EthClient::new(Url::parse(&chain.url).unwrap()).unwrap();
        let mock_a = Arc::new(mock::spawn(mock::Behaviour::Ack).await);
        let mut harness = Harness::spawn(&[("a", &mock_a.url)], Duration::from_millis(20));

        let secret = secp256k1::SecretKey::from_slice(&[0x33; 32]).unwrap();
        let lane = U256::from(7);
        // What a custom pricer's build gets: the feed is taken from the context, and the
        // context's sender goes into the lane's source, as `build_custom` wires it.
        let values = ValueSource::fixed_for_tests(U256::one(), scaled(100).1, 6);
        let ctx = BuildCtx::new(values.pair_for_tests().as_ref().clone());
        let feed = ctx.landings();
        let values = values.with_landings(ctx.landings_sender());
        assert_eq!(
            feed.latest(),
            None,
            "nothing has landed before the first block"
        );
        let live = Live {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane,
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TEST".to_owned(),
                band: MidBand {
                    min: None,
                    max: None,
                },
            },
            values,
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: test_pair_metrics(),
            venues: Vec::new(),
            observers: Default::default(),
        };
        let params = UpdateParams {
            registry: Address::from_slice(&[0x77; 20]),
            target: Address::from_slice(&[0x99; 20]),
            lane,
            chain_id: 31337,
        };
        let opts = SendOpts {
            registry: params.registry,
            target: params.target,
            chain_id: 31337,
            no_pin: false,
            mine: false,
            once: true,
            requote_ms: 20,
            disable_cross_region: false,
            price_decimals: 6,
            recorder: None,
        };
        let names = vec!["a".to_owned()];
        let head_at = |number: u64| Head {
            number,
            timestamp: wall_clock_secs(),
            base_fee_per_gas: Some(1_000_000_000),
        };
        let (heads, mut head) = watch::channel(head_at(99));
        // As in the test above: block 100 passes once the builder holds the quote for it.
        let landed = {
            let mock_a = mock_a.clone();
            tokio::spawn(async move {
                assert!(mock_a.wait_for(1, Duration::from_secs(5)).await);
                tokio::time::sleep(Duration::from_millis(100)).await;
                heads.send_replace(head_at(100));
            })
        };
        let started = std::time::Instant::now();
        let ended = tokio::time::timeout(
            Duration::from_secs(5),
            drive(
                &client,
                &live,
                &params,
                &harness.cmd,
                &mut harness.events,
                &names,
                &opts,
                &mut head,
                shutdown_channel().1,
            ),
        )
        .await
        .expect("drive must return once the target block has passed");
        assert_eq!(ended.unwrap(), Ended::Finished);
        landed.await.unwrap();

        let report = feed
            .latest()
            .expect("the block's read-back reached the feed");
        assert_eq!(report.block, 100);
        // The stub answers eth_call with nothing, which is an unverifiable read-back.
        assert_eq!(report.landing, LandingOutcome::Unknown);
        assert!(report.at >= started);
        harness.shutdown().await;
    }

    fn fresh_gate() -> PublishGate {
        PublishGate::new(Latch::new())
    }

    /// What the composite task holds for a lane: the deviation guard and the latch its
    /// verdicts trip. Shared so a test can keep the handle a gate was built from, the way
    /// `Live` does across a supervised restart.
    struct Judge {
        latch: Latch,
        guard: crate::guard::deviation::DeviationGuard,
    }

    type SharedJudge = Arc<std::sync::Mutex<Judge>>;

    fn judge_with(config: BreakerConfig) -> SharedJudge {
        Arc::new(std::sync::Mutex::new(Judge {
            latch: Latch::new(),
            guard: crate::guard::deviation::DeviationGuard::new(config, None),
        }))
    }

    /// The lane's latch, which is all a gate reads.
    fn latch_of(judge: &SharedJudge) -> Latch {
        judge.lock().unwrap().latch.clone()
    }

    /// A 2% guard at 6 decimals.
    fn two_percent() -> SharedJudge {
        judge_with(BreakerConfig {
            threshold_scaled: Some(U256::from(20_000)),
            window: None,
            decimals: 6,
        })
    }

    /// A guard with no tick limit and a 10% window over 5 blocks, so only the windowed
    /// check can end the loop and the halt under test is unambiguously the window's.
    fn ten_percent_window() -> SharedJudge {
        judge_with(BreakerConfig {
            threshold_scaled: None,
            window: Some(crate::breaker::WindowConfig {
                threshold_scaled: crate::feed::parse_decimal_scaled("0.10", 6).unwrap(),
                blocks: 5,
                span: std::time::Duration::from_secs(60),
            }),
            decimals: 6,
        })
    }

    fn breaker_gate() -> PublishGate {
        PublishGate::new(latch_of(&two_percent()))
    }

    /// What the composite task does with each sample it reads: judges it, whether or not a
    /// loop is there to ask, and trips the latch on the verdict. The gate never judges a
    /// price itself.
    ///
    /// `step_secs` lands the whole call that many seconds after `Instant::now()`, so a test
    /// driving a windowed guard can put two calls a measurable distance apart, the same
    /// second-granularity bucketing a real feed's samples fall into. Every caller that does
    /// not care about the window passes 0, landing at effectively the same instant as before.
    fn judge(judge: &SharedJudge, mids: &[u64], step_secs: u64) {
        use crate::guard::{Cause, CompositeSample, MarketGuard, Verdict};
        let at = std::time::Instant::now() + Duration::from_secs(step_secs);
        let mut j = judge.lock().unwrap();
        for mid in mids {
            let sample =
                CompositeSample::new(at, scaled(*mid).1, U256::zero(), false, 6, Vec::new());
            if let Verdict::Trip(reason) = j
                .guard
                .assess(&sample, &mut crate::pricing::Diagnostics::new(0))
            {
                j.latch.trip("deviation", Cause::Guard, reason);
            }
        }
    }

    /// The gate does not judge prices — the feed does, where every sample is seen. There is
    /// deliberately one judge: a second one here would move the reference under the first
    /// (the loop reads the latest sample, which the feed may already have moved past), and
    /// two writers to one reference can manufacture a trip out of two harmless steps. So a
    /// price nobody judged is published even when it is a jump; the only way that happens
    /// is a caller bypassing the feed.
    #[test]
    fn the_gate_publishes_whatever_the_feed_has_not_judged() {
        let mut gate = breaker_gate();
        gate.decide(Ok(scaled(100)));
        gate.published(scaled(100));
        assert_eq!(
            gate.decide(Ok(scaled(103))).action,
            Action::Publish(scaled(103)),
            "the gate must not judge prices itself"
        );
    }

    fn scaled(value: u64) -> (U256, U256) {
        (U256::from(1), U256::from(value) * U256::exp10(6))
    }

    fn pair(delta: u64, mid: u64) -> (U256, U256) {
        (U256::from(delta), U256::from(mid))
    }

    /// The dedup cache: the tasks requote the standing command themselves, so an
    /// unchanged price must not re-sign and re-publish every tick.
    #[test]
    fn a_fresh_price_publishes_and_an_unchanged_one_is_skipped() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
        gate.published(pair(1, 100));
        assert_eq!(gate.decide(Ok(pair(1, 100))).action, Action::Nothing);
        assert_eq!(
            gate.decide(Ok(pair(1, 101))).action,
            Action::Publish(pair(1, 101))
        );
    }

    /// A failed signing attempt (an RPC blip) must retry on the next tick, so it must not
    /// be remembered as published: only `published` updates the cache.
    #[test]
    fn a_failed_signing_attempt_is_retried_next_tick() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
        // published() not called: the sign failed.
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
    }

    /// The episode latch: the builders' own tickers keep a retraction in force, so the
    /// withdraw goes out once per episode, not on every tick — and the first reason is
    /// the one the block header reports.
    #[test]
    fn an_unusable_price_withdraws_once_per_episode() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Err(eyre!("stale price"))).action,
            Action::Withdraw
        );
        assert_eq!(
            gate.decide(Err(eyre!("still stale"))).action,
            Action::Nothing
        );
        assert_eq!(gate.withdrawn_reason(), Some("stale price"));
    }

    /// Recovery ends the episode: the next good price publishes and the header goes back
    /// to reporting slots. An unusable price is not a breaker trip — the feed going quiet
    /// is ordinary, and recovering from it must stay automatic.
    #[test]
    fn publishing_after_an_outage_clears_the_episode() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Err(eyre!("stale price"))).action,
            Action::Withdraw
        );
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
        gate.published(pair(1, 100));
        assert_eq!(gate.withdrawn_reason(), None);
        assert_eq!(gate.quoted(), Some(pair(1, 100)));
    }

    /// Starting an episode clears the dedup cache: a withdraw destroyed the quote at the
    /// builders, so "unchanged" must not swallow a resumption at the very same price.
    #[test]
    fn recovery_at_the_same_price_republishes_within_a_block() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
        gate.published(pair(1, 100));
        assert_eq!(
            gate.decide(Err(eyre!("stale price"))).action,
            Action::Withdraw
        );
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
    }

    /// Every block needs its own signature (new stamp, new nonce) and owns its own
    /// withdraw episode, so the per-block state is forgotten at rollover.
    #[test]
    fn begin_block_forgets_the_block_state() {
        let mut gate = fresh_gate();
        gate.decide(Ok(pair(1, 100)));
        gate.published(pair(1, 100));
        gate.begin_block();
        assert_eq!(
            gate.decide(Ok(pair(1, 100))).action,
            Action::Publish(pair(1, 100))
        );
        let mut gate = fresh_gate();
        gate.decide(Err(eyre!("stale price")));
        gate.begin_block();
        assert_eq!(
            gate.decide(Err(eyre!("stale price"))).action,
            Action::Withdraw
        );
    }

    /// The feature end to end at the gate level: a jump the feed judged withdraws the
    /// quote and halts the pair, announcing why, and the block header reports the
    /// deviation rather than whatever else may have been wrong.
    #[test]
    fn a_price_jump_halts_the_pair() {
        let breaker = two_percent();
        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        assert_eq!(
            gate.decide(Ok(scaled(100))).action,
            Action::Publish(scaled(100))
        );
        gate.published(scaled(100));
        judge(&breaker, &[103], 0);
        let tripped = gate.decide(Ok(scaled(103)));
        assert_eq!(tripped.action, Action::Halt);
        assert!(
            tripped
                .announce
                .is_some_and(|line| line.contains("tripped") && line.contains("3.00%")),
        );
        assert!(
            gate.withdrawn_reason()
                .is_some_and(|r| r.contains("deviation"))
        );
        // The quote is not left standing in the header as though it were still live.
        assert_eq!(gate.quoted(), None);
    }

    /// The difference from the auto-re-arming design, at the level drive actually sees:
    /// no later price, and no block rollover, can produce anything but another Halt.
    /// drive returns on the first one, so this is the belt to the breaker's braces.
    #[test]
    fn a_halted_gate_never_publishes_again() {
        let breaker = two_percent();
        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        gate.decide(Ok(scaled(100)));
        gate.published(scaled(100));
        judge(&breaker, &[103], 0);
        assert_eq!(gate.decide(Ok(scaled(103))).action, Action::Halt);
        // The price returns to exactly where it was and stays there, across blocks, with
        // the feed judging every calm sample on the way.
        for _ in 0..10 {
            judge(&breaker, &[100], 0);
            assert_eq!(gate.decide(Ok(scaled(100))).action, Action::Halt);
            gate.begin_block();
            assert_eq!(gate.decide(Ok(scaled(100))).action, Action::Halt);
        }
    }

    /// One latch, two writers: a trip arriving mid-outage still halts and takes over the
    /// header reason, because it is the more specific truth — and because the ordinary
    /// withdraw-once throttle must not swallow the end of the pair.
    #[test]
    fn a_trip_during_a_feed_outage_still_halts() {
        let breaker = two_percent();
        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        gate.decide(Ok(scaled(100)));
        gate.published(scaled(100));
        assert_eq!(
            gate.decide(Err(eyre!("stale price"))).action,
            Action::Withdraw
        );
        judge(&breaker, &[103], 0);
        let tripped = gate.decide(Ok(scaled(103)));
        assert_eq!(tripped.action, Action::Halt);
        assert!(
            tripped
                .announce
                .is_some_and(|line| line.contains("tripped"))
        );
        assert!(
            gate.withdrawn_reason()
                .is_some_and(|r| r.contains("deviation")),
            "the trip reason must win over the outage's"
        );
    }

    /// Regression test for the restart hole found in review: `supervise` rebuilds the
    /// quote loop on any error, so a breaker owned by the loop came back with no reference
    /// and the first price after a restart was taken on faith. Since the failure that
    /// triggers a restart (every builder dropping, an RPC blip) is exactly the kind of
    /// event a dislocation travels with, that is the worst possible moment to forget. The
    /// feed keeps judging while no loop exists, and the rebuilt gate finds its verdict.
    #[test]
    fn a_jump_while_the_loop_was_being_restarted_halts_the_rebuilt_gate() {
        let breaker = two_percent();

        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        assert_eq!(
            gate.decide(Ok(scaled(100))).action,
            Action::Publish(scaled(100))
        );
        gate.published(scaled(100));

        // The task dies and `supervise` waits out a backoff before restarting it with a
        // brand-new gate. The feed does not stop for that.
        drop(gate);
        judge(&breaker, &[103], 0);
        let mut restarted = PublishGate::new(latch_of(&breaker));

        // 3% from the mid published before the restart. A gate with a fresh breaker would
        // treat this as its first price and publish it unguarded.
        assert_eq!(restarted.decide(Ok(scaled(103))).action, Action::Halt);
    }

    /// The other half of the same guarantee: a trip survives a restart too, so a pair
    /// cannot be brought back to life by whatever knocked its task over.
    #[test]
    fn a_trip_survives_a_rebuilt_gate() {
        let breaker = two_percent();
        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        gate.decide(Ok(scaled(100)));
        gate.published(scaled(100));
        judge(&breaker, &[103], 0);
        assert_eq!(gate.decide(Ok(scaled(103))).action, Action::Halt);

        drop(gate);
        // Back at the pre-trip price, which a fresh breaker would happily quote.
        judge(&breaker, &[100], 0);
        let mut restarted = PublishGate::new(latch_of(&breaker));
        assert_eq!(restarted.decide(Ok(scaled(100))).action, Action::Halt);
    }

    /// A tripped gate must answer Halt on the stale-feed path too. `drive` returns on the
    /// first Halt, so this is not reachable through it today — but the guard's promise is
    /// that nothing talks it back into quoting, and a caller that kept a gate alive past a
    /// trip would otherwise get an ordinary withdraw and resume when the feed recovered.
    #[test]
    fn a_tripped_gate_halts_on_an_unusable_price_too() {
        let breaker = two_percent();
        let mut gate = PublishGate::new(latch_of(&breaker));
        judge(&breaker, &[100], 0);
        gate.decide(Ok(scaled(100)));
        gate.published(scaled(100));
        judge(&breaker, &[103], 0);
        assert_eq!(gate.decide(Ok(scaled(103))).action, Action::Halt);

        // A new block clears the per-block latch but not the breaker.
        gate.begin_block();
        assert_eq!(gate.decide(Err(eyre!("stale price"))).action, Action::Halt);
        assert!(
            gate.withdrawn_reason()
                .is_some_and(|r| r.contains("deviation")),
            "the halt reason must outrank the stale-feed one"
        );
    }

    /// A pair with no `max_deviation` has no breaker at all: the same jump that halts a
    /// guarded pair is just another price, since opting in is what arms the guard.
    #[test]
    fn an_unguarded_pair_never_halts() {
        let mut gate = fresh_gate();
        assert_eq!(
            gate.decide(Ok(scaled(100))).action,
            Action::Publish(scaled(100))
        );
        gate.published(scaled(100));
        assert_eq!(
            gate.decide(Ok(scaled(500))).action,
            Action::Publish(scaled(500))
        );
    }

    /// Pins the mapping from every `Action` variant to its `PublishAction` label. A free
    /// function with its own test rather than an inline match arm at the recording site, so
    /// a new `Action` variant fails to compile here instead of quietly falling into an
    /// existing bucket and making a dashboard lie.
    #[test]
    fn every_gate_action_maps_to_its_own_label() {
        assert_eq!(
            action_label(&Action::Publish((U256::one(), U256::one()))),
            PublishAction::Publish
        );
        assert_eq!(action_label(&Action::Withdraw), PublishAction::Withdraw);
        assert_eq!(action_label(&Action::Halt), PublishAction::Halt);
        assert_eq!(action_label(&Action::Nothing), PublishAction::Nothing);
    }
}
