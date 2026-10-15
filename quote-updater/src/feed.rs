//! The market-data feed: maintains the latest (delta, mid) quote of one market at one
//! venue — the midpoint of its best bid/ask and the half-spread fraction — scaled to
//! integers. The wire (URL, subscribe message, frame format, keepalive) is the venue's,
//! see `venue.rs`; everything from the frame onwards is here and the same for all of them.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use ethrex_common::U256;
use eyre::{Result, WrapErr, ensure, eyre};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use crate::metrics::{TickReject, VenueMetrics};
use crate::record::Recorder;
use crate::venue::{Connection, Keepalive, Quote, VenueId};
use crate::ws;

/// Whether a venue is connected, and when it last priced, held by its feed task, which is
/// the one that knows, and read by the metrics when Prometheus scrapes
/// (`Metrics::watch_venue`) and by the quote loop's source checks (`SourceGate`). Nothing
/// about it is written ahead of time, so nothing about it can be lost: a restart, a reload,
/// a write the gate dropped, none of them can leave `quote_updater_venue_feed_up` or
/// `quote_updater_venue_source_age_ms` saying the wrong thing.
#[derive(Clone, Default)]
pub struct Connected {
    up: Arc<AtomicU8>,
    /// When the venue last delivered a price this feed accepted; see [`PriceClock`].
    priced: Arc<std::sync::Mutex<Option<PriceClock>>>,
}

/// When a venue's latest accepted price was made: when its frame arrived, on both clocks,
/// and how much later than usual it arrived behind the exchange's own stamp.
///
/// Separate from [`PriceSample::at`], and on purpose: the sample's stamp is refreshed by
/// every ping a live socket answers, which is what keeps a quiet book publishable for
/// [`crate::update::MAX_PRICE_AGE`]. That is also why it cannot see a stream that is alive
/// but late: frames keep arriving, each one old, and each one re-stamps the sample as new.
/// This clock moves only on a price, and carries the exchange's time when the frame does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PriceClock {
    /// When the frame carrying the price arrived, on the monotonic clock.
    pub received_at: Instant,
    /// The same, on the wall clock.
    pub received_wall: SystemTime,
    /// How far the arrival trailed the exchange's stamp, over the least it has trailed it
    /// lately ([`LagFloor`]): `None` for a venue whose frames carry no time, whose price is
    /// then aged by its arrival alone.
    pub lag: Option<Duration>,
}

impl PriceClock {
    /// A price that arrived at `(received_at, received_wall)`, `lag` behind its usual lag.
    pub fn new(received_at: Instant, received_wall: SystemTime, lag: Option<Duration>) -> Self {
        Self {
            received_at,
            received_wall,
            lag,
        }
    }

    /// How old the price is at `(now, wall)`: its age since arrival plus how much later
    /// than usual it arrived behind the exchange's stamp, for a venue that stamps its
    /// frames; its age since arrival alone for one that does not. The arrival's age is read
    /// by whichever clock says older, as [`PriceSample::age_at`] reads a sample's.
    pub fn age_at(&self, now: Instant, wall: SystemTime) -> Duration {
        let monotonic = now.saturating_duration_since(self.received_at);
        let since_arrival =
            monotonic.max(wall.duration_since(self.received_wall).unwrap_or(monotonic));
        since_arrival + self.lag.unwrap_or_default()
    }

    /// The monotonic instant from which [`Self::age_at`] exceeds `limit`, for a timer:
    /// already past for a price that arrived later than `limit` after its own stamp.
    pub fn stale_after(&self, limit: Duration) -> Instant {
        self.received_at + limit.saturating_sub(self.lag.unwrap_or_default())
    }

    /// Whether the age is measured from the exchange's stamp (`true`) or the arrival.
    pub fn by_exchange_time(&self) -> bool {
        self.lag.is_some()
    }
}

/// How long [`LagFloor`] remembers a venue's least lag. A backlog shorter than this is
/// measured in full; a skew or a baseline latency that holds for longer is subtracted out.
pub(crate) const LAG_FLOOR_WINDOW: Duration = Duration::from_secs(60 * 60);

/// The resolution of that memory: one minimum per this much time.
const LAG_FLOOR_BUCKET: Duration = Duration::from_secs(60);

/// The least lag a venue's stamped frames have arrived with over [`LAG_FLOOR_WINDOW`], and
/// each new frame's lag over it.
///
/// `arrival wall clock - exchange stamp` is not a frame's lateness alone: it also carries
/// our clock's offset from the exchange's (ahead adds to every frame, behind subtracts) and
/// the network's ordinary latency. Both hold steady over an hour where a backlog does not,
/// so subtracting the hour's minimum leaves the lateness: a host whose clock is 5 seconds
/// ahead does not read every OKX price as 5 seconds old, and one 5 seconds behind does not
/// hide a 5-second backlog. Signed, for that second case. The cost: the first frame after
/// the feed starts sets the floor, so a venue already late when it is dialled reads as on
/// time until it catches up once.
#[derive(Debug, Default)]
pub(crate) struct LagFloor {
    /// `(bucket start, least lag in it in ms)`, oldest first.
    buckets: std::collections::VecDeque<(Instant, i64)>,
}

impl LagFloor {
    /// Records a frame stamped `event` that arrived at `(now, wall)`, and returns its lag
    /// over the floor.
    pub(crate) fn lag(&mut self, now: Instant, wall: SystemTime, event: SystemTime) -> Duration {
        let raw = signed_epoch_ms(wall) - signed_epoch_ms(event);
        match self.buckets.back_mut() {
            Some((start, least)) if now.saturating_duration_since(*start) < LAG_FLOOR_BUCKET => {
                *least = (*least).min(raw);
            }
            _ => self.buckets.push_back((now, raw)),
        }
        while self
            .buckets
            .front()
            .is_some_and(|(start, _)| now.saturating_duration_since(*start) >= LAG_FLOOR_WINDOW)
        {
            self.buckets.pop_front();
        }
        let floor = self
            .buckets
            .iter()
            .map(|(_, least)| *least)
            .min()
            .unwrap_or(raw);
        Duration::from_millis(u64::try_from(raw - floor).unwrap_or(0))
    }
}

/// Milliseconds since the epoch, negative before it.
fn signed_epoch_ms(at: SystemTime) -> i64 {
    match at.duration_since(std::time::UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_millis()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

const DOWN: u8 = 0;
const UP: u8 = 1;
/// The feed task has ended: its pair is finished (a halt), so there is no feed to be up or
/// down, which the gauge says as NaN, the same as a pair that never had one.
const GONE: u8 = 2;

impl Connected {
    fn set(&self, up: bool) {
        self.up.store(if up { UP } else { DOWN }, Ordering::Relaxed);
    }

    fn gone(&self) {
        self.up.store(GONE, Ordering::Relaxed);
    }

    /// What `quote_updater_venue_feed_up` reads: 1 connected, 0 not, NaN no feed task.
    pub fn gauge_value(&self) -> f64 {
        match self.up.load(Ordering::Relaxed) {
            UP => 1.0,
            DOWN => 0.0,
            _ => f64::NAN,
        }
    }

    /// Records the clock of a price the feed just accepted.
    pub(crate) fn priced(&self, clock: PriceClock) {
        *self.priced.lock().unwrap_or_else(|e| e.into_inner()) = Some(clock);
    }

    /// When the venue last delivered a price, or `None` before its first.
    pub fn price_clock(&self) -> Option<PriceClock> {
        *self.priced.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// What `quote_updater_venue_source_age_ms` reads: the latest price's age in
    /// milliseconds (see [`PriceClock::age_at`]), NaN before the first price and once the
    /// feed task is gone.
    pub fn source_age_ms(&self, now: Instant, wall: SystemTime) -> f64 {
        if self.up.load(Ordering::Relaxed) == GONE {
            return f64::NAN;
        }
        self.price_clock().map_or(f64::NAN, |clock| {
            clock.age_at(now, wall).as_secs_f64() * 1000.0
        })
    }

    /// A flag already set, for tests of what reads it.
    #[cfg(test)]
    pub fn with(up: bool) -> Self {
        let flag = Self::default();
        flag.set(up);
        flag
    }
}

/// A handle on some of a pair's metrics, and whether the holder is the generation that
/// owns them. `M` is [`VenueMetrics`] in a venue feed and `PairMetrics` in the composite
/// that averages the venues; one [`Adoption`] switch per pair generation gates them all.
///
/// A reload dials the replacement feeds **before** stopping the pair it replaces — that gap
/// is the whole no-downtime property — so for a few seconds two generations hold the same
/// label's series. Both write the *same* gauges, and only one of them is describing the
/// pair the service is actually quoting.
///
/// Without this gate the loser writes anyway, and always last:
///
/// - The replacement connects and sets `feed_up` to 1, then the outgoing feed's `tx.closed()`
///   arm sets it to NaN. Nothing writes `feed_up` again until a redial, so a healthy,
///   freshly reloaded lane reads as having no feed for as long as its socket stays up —
///   possibly days — and `PusherFeedDown` cannot fire for it.
/// - A replacement whose feed never comes up is worse. The lane keeps quoting on its old
///   config, exactly as the reload promises, but the failed dial's `feed_up.set(0.0)` and
///   its teardown NaNs land on the running pair's series on the way out.
///
/// So a feed writes nothing until [`Service::spawn`] adopts it, and stops writing the moment
/// [`Service::stop`] lets it go. Only the metrics are gated: the samples and the reconnect
/// loop are the feed's real work and run either way.
#[derive(Clone)]
pub struct Gated<M> {
    inner: M,
    adopted: Adoption,
}

impl<M> Gated<M> {
    /// `inner`, written only while `adopted` is raised.
    pub fn new(inner: M, adopted: Adoption) -> Self {
        Self { inner, adopted }
    }

    /// Runs `write` against the series, if this generation still owns them.
    ///
    /// A closure under the lock rather than a borrow the caller decides what to do with,
    /// because the check and the write have to be one step. A plain flag leaves a window —
    /// the feed reads "yes", is preempted, the service releases it and hands the series to
    /// the replacement, and the write then lands on the new generation's gauges. Narrow, but
    /// it is the *same* silent blinding of PusherFeedDown this type exists to prevent, and
    /// on a service that reloads for months narrow is not never.
    ///
    /// Nothing inside may await, which taking a closure rather than a guard makes structural
    /// rather than a rule to remember: this is a std lock held across the whole of `write`,
    /// and [`Adoption::release`] blocks on it.
    pub fn record(&self, write: impl FnOnce(&M)) {
        // A panic inside a `write` would poison this for the life of the process, silencing
        // a lane's metrics on a lock that is only ever guarding a handful of atomic stores.
        let adopted = self.adopted.0.read().unwrap_or_else(|e| e.into_inner());
        if *adopted {
            write(&self.inner);
        }
    }
}

/// Hands one pair generation's metric series to its feeds, and takes them back.
///
/// Held by the service rather than the feed: [`Service::spawn`] adopts a generation once it
/// is the one serving the lane, and [`Service::stop`] releases it before the replacement is
/// adopted. `release` waits for any write already inside [`Gated::record`] to finish, so
/// once it returns the released generation can no longer touch the series at all.
#[derive(Clone, Default)]
pub struct Adoption(Arc<RwLock<bool>>);

impl Adoption {
    /// Lowered: nothing gated on it writes yet.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn adopt(&self) {
        self.set(true);
    }

    pub fn release(&self) {
        self.set(false);
    }

    fn set(&self, owns: bool) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = owns;
    }
}

/// How long a connection may stay silent before it is probed with a ping. bookTicker
/// emits only when the best bid/ask changes, so silence alone does not mean the
/// connection is dead — a quiet book on an illiquid pair is healthy.
const PING_INTERVAL: Duration = Duration::from_secs(10);

/// How long a connection may go without any inbound frame (price, ping, or pong) before
/// it is considered dead and redialed. Pings go out every PING_INTERVAL, so a live
/// socket answers well within this bound even while the book itself is quiet.
const MAX_QUIET: Duration = Duration::from_secs(30);

/// Delay between reconnect attempts.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// How long one polled fetch may take. Well under the poll interval of the slowest polled
/// venue, so a hung request cannot stack up behind the next one.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// A quote sampled from the stream, stamped with its arrival time so consumers can
/// refuse to publish a value from a feed that has silently stopped. Stamped with both
/// clocks: `at` (monotonic) for ordinary staleness immune to clock steps, and `wall`
/// because Instant pauses during system suspend, so only the wall clock can age out a
/// pre-sleep sample on wake.
#[derive(Clone, Copy, Debug)]
pub struct PriceSample {
    /// The book's half-spread as a fraction of the mid, scaled by 10^decimals.
    pub delta: U256,
    /// Best bid/ask midpoint, scaled by 10^decimals.
    pub mid: U256,
    pub at: Instant,
    pub wall: SystemTime,
}

impl PriceSample {
    /// How old this sample is at `(now, wall)`, by whichever clock says older.
    ///
    /// Instant pauses during system suspend, so a pre-sleep sample can look seconds old
    /// on wake; the wall clock keeps counting through suspend and catches that. If the
    /// wall clock stepped backwards instead (NTP), it cannot be trusted and the monotonic
    /// age alone governs.
    pub fn age_at(&self, now: Instant, wall: SystemTime) -> Duration {
        let monotonic_age = now.saturating_duration_since(self.at);
        let wall_age = wall.duration_since(self.wall).unwrap_or(monotonic_age);
        monotonic_age.max(wall_age)
    }

    pub fn age(&self) -> Duration {
        self.age_at(Instant::now(), SystemTime::now())
    }
}

/// Spawns the stream reader task; the returned channel always holds the latest sample.
/// The task redials on any failure for as long as anyone holds the receiver — staleness,
/// not connection state, is the consumer's signal that the feed is unhealthy (via
/// [`PriceSample::at`]) — and exits once every receiver is gone, which is what a halted
/// pair looks like from here.
///
/// A venue feed judges nothing and averages nothing: the breaker and the σ history belong
/// to the composite that reads it (see `composite.rs`), because what they must see is the
/// number the lane publishes. `recorder`, when given, receives every accepted market mid
/// under [`Recorder::mid_key`] for the P&L dashboard's per-venue series.
#[allow(clippy::too_many_arguments)] // two run sites (the composite, the preview), each naming them
pub fn spawn_feed(
    http: Arc<reqwest::Client>,
    venue: VenueId,
    endpoint: &str,
    symbol: &str,
    decimals: u32,
    invert: bool,
    metrics: Gated<VenueMetrics>,
    recorder: Option<Recorder>,
    connected: Connected,
) -> watch::Receiver<Option<PriceSample>> {
    let (tx, rx) = watch::channel(None);
    let feed = Feed {
        venue,
        endpoint: endpoint.to_owned(),
        http,
        // Named on every line this task prints: with one feed per venue per pair, an
        // unattributed "stream error" cannot be told apart from the other feeds, and this
        // is the component whose silent failure takes a venue out of a lane's average.
        symbol: symbol.to_owned(),
        decimals,
        invert,
        tx,
        metrics,
        recorder,
        connected,
        lag_floor: Default::default(),
    };
    tokio::spawn(async move {
        let Feed {
            venue,
            symbol,
            tx,
            metrics,
            ..
        } = &feed;
        // Every receiver being gone means the pair this feeds has stopped for good — a
        // circuit-breaker halt, in practice. Without this the socket and its reconnect loop
        // outlive the lane they serve, and a halted pusher is designed to sit for days, so
        // it would hold a websocket open the whole time for a pair that will never quote
        // again. Raced against the whole loop rather than checked between attempts, so a
        // halt lands during a long-lived stream as well as during a reconnect wait — and
        // written once, so there is a single place where a lane's feed goes away.
        tokio::select! {
            // The feed task itself is ending, so its three gauges get exactly the values
            // `build_live` writes for a pair that never had a feed at all: that is now the
            // truth, and it is the one reading no rule mistakes for a live feed.
            //
            // 0.0 would be defensible for `up` alone — the feed *is* down — but this arm
            // only ever runs when a pair is finished, which in practice means a breaker halt.
            // PusherBreakerTripped already pages on that, latching until a human clears it,
            // and the frozen `last_tick` would then age out into PusherFeedStale as well, so
            // one halt would raise a page and two tickets about the same fact. NaN for all
            // three says "this pair has no feed task", which is exactly what happened.
            //
            // Distinct from the `set(0.0)` inside the loop below: there the task still
            // exists and is about to redial, which is a feed that is down rather than a feed
            // that is gone.
            _ = tx.closed() => {
                feed.connected.gone();
                metrics.record(|m| {
                    m.last_tick.set(f64::NAN);
                    m.sample_current.set(f64::NAN);
                });
            }
            _ = async {
                loop {
                    // Resolved per attempt rather than once: a venue that hands out
                    // per-session URLs needs a fresh one on every redial.
                    let result = match venue.connection(&feed.endpoint, symbol, &feed.http).await {
                        Ok(Connection::Stream {
                            url,
                            subscribe,
                            keepalive,
                        }) => feed.read_stream(&url, subscribe.as_deref(), &keepalive).await,
                        Ok(Connection::Poll { url, headers, every }) => {
                            feed.poll(&url, &headers, every).await
                        }
                        Err(err) => Err(err),
                    };
                    // The stream is gone the instant `read_stream` returns, on either arm —
                    // a clean close and a hard error both mean "not connected" — so the flag
                    // must not go on saying connected through the reconnect wait below.
                    feed.set_connected(false);
                    match result {
                        Ok(()) => tracing::warn!("[{symbol}] {venue} stream closed; reconnecting"),
                        Err(err) => {
                            tracing::warn!("[{symbol}] {venue} feed error: {err:#}; reconnecting")
                        }
                    }
                    tokio::time::sleep(RECONNECT_DELAY).await;
                }
            } => {}
        }
    });
    rx
}

/// The run's HTTP client: one for every polled venue, the venue whose socket URL is fetched
/// over HTTP, and every pricer's build (`BuildCtx::http`), so the process keeps one
/// connection pool. Every request times out after [`POLL_TIMEOUT`] unless it sets a
/// timeout of its own, and names the pusher: CoinGecko (the vault's USD prices) and
/// Coinbase answer 403 to a request without a User-Agent.
pub(crate) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(POLL_TIMEOUT)
        .user_agent(concat!("quote-updater/", env!("CARGO_PKG_VERSION")))
        .build()
        .expect("a default reqwest client builds")
}

/// One feed's constants: what it reads, how it scales it, and where the samples go.
struct Feed {
    venue: VenueId,
    endpoint: String,
    /// For polled venues, and for the venue whose socket URL is fetched over HTTP: the
    /// run's one client (see [`http_client`]), so every feed draws on one connection pool.
    http: Arc<reqwest::Client>,
    symbol: String,
    decimals: u32,
    invert: bool,
    tx: watch::Sender<Option<PriceSample>>,
    metrics: Gated<VenueMetrics>,
    recorder: Option<Recorder>,
    /// Whether this venue is connected right now; see [`Connected`].
    connected: Connected,
    /// The venue's usual lag behind its own stamps, across reconnects; see [`LagFloor`].
    lag_floor: std::sync::Mutex<LagFloor>,
}

impl Feed {
    fn set_connected(&self, connected: bool) {
        self.connected.set(connected);
    }
}

impl Feed {
    /// Reads one connection until it closes, errors, or goes quiet past MAX_QUIET.
    async fn read_stream(
        &self,
        url: &str,
        subscribe: Option<&str>,
        keepalive: &Keepalive,
    ) -> Result<()> {
        let Feed {
            venue, symbol, tx, ..
        } = self;
        // Bounded by ws::CONNECT_TIMEOUT: without it a blackholed endpoint holds each redial
        // hostage for the OS connect timeout (minutes), stretching RECONNECT_DELAY's intended
        // cadence into long dead periods.
        let mut ws = ws::connect(url).await?;
        tracing::info!("[{symbol}] {venue} stream connected: {url}");
        if let Some(subscribe) = subscribe {
            ws.send(Message::text(subscribe))
                .await
                .wrap_err("subscribe failed")?;
        }
        self.set_connected(true);
        let mut last_inbound = Instant::now();
        // Whether the sample in the channel is still backed by a ticker this connection
        // accepted. A rejected ticker (one-sided or crossed book) leaves the previous sample
        // in place, and from that moment on it no longer describes a book anyone can trade —
        // so it must be allowed to age out. See the Ping/Pong arm below.
        let mut sample_is_current = false;
        loop {
            let msg = match tokio::time::timeout(PING_INTERVAL, ws.next()).await {
                Ok(msg) => msg,
                Err(_) => {
                    ensure!(
                        last_inbound.elapsed() < MAX_QUIET,
                        "no message in {MAX_QUIET:?}"
                    );
                    // A quiet book emits nothing, so probe the socket instead of assuming
                    // the worst; any answering frame below counts as feed liveness.
                    let probe = match keepalive.frame() {
                        None => Message::Ping(Vec::new().into()),
                        Some(text) => Message::text(text),
                    };
                    ws.send(probe).await.wrap_err("ping failed")?;
                    continue;
                }
            };
            let Some(msg) = msg else {
                return Ok(());
            };
            last_inbound = Instant::now();
            match msg? {
                Message::Text(text) => self.on_frame(&text, &mut sample_is_current),
                Message::Close(_) => return Ok(()),
                // A live socket with no book change means the last mid is still current
                // (bookTicker emits on every change), so liveness refreshes the sample.
                // tungstenite answers the server's pings by itself while the stream is polled.
                //
                // Gated on `sample_is_current`, and that gate is the whole point: once a
                // ticker has been rejected, the retained sample describes a book that no
                // longer exists. Refreshing it here would reset MAX_PRICE_AGE on every
                // inbound frame — and Binance pings well inside that window — so a book stuck
                // one-sided or crossed would keep an unpublishable price looking fresh
                // indefinitely, which is exactly what the staleness guard exists to stop.
                Message::Ping(_) | Message::Pong(_) => {
                    refresh_if_current(tx, sample_is_current);
                }
                _ => {}
            }
        }
    }
}

impl Feed {
    /// Fetches the resource every `every` for as long as the fetches keep working: a polled
    /// venue's equivalent of one connection. A failed fetch ends the "connection", so the
    /// caller's redial loop reports it and comes back after RECONNECT_DELAY, and the
    /// retained sample ages out on its own if that keeps happening. A body that does not
    /// parse is a rejected frame, counted and skipped, not a failed fetch: the venue is
    /// answering, just not with a price.
    async fn poll(&self, url: &str, headers: &[(String, String)], every: Duration) -> Result<()> {
        let Feed {
            venue,
            symbol,
            http,
            ..
        } = self;
        let mut sample_is_current = false;
        let mut first = true;
        loop {
            let mut request = http.get(url);
            for (name, value) in headers {
                request = request.header(name.as_str(), value.as_str());
            }
            let body = request
                .send()
                .await
                .wrap_err("request failed")?
                .error_for_status()
                .wrap_err("request refused")?
                .text()
                .await
                .wrap_err("body unreadable")?;
            if first {
                tracing::info!("[{symbol}] {venue} polling: {url}");
                self.set_connected(true);
                first = false;
            }
            self.on_frame(&body, &mut sample_is_current);
            tokio::time::sleep(every).await;
        }
    }

    /// One inbound text, from a stream frame or a polled body, through the venue's parser
    /// and the shared scaling into the channel and the metrics.
    fn on_frame(&self, text: &str, sample_is_current: &mut bool) {
        let Feed {
            venue,
            symbol,
            decimals,
            invert,
            tx,
            metrics,
            recorder,
            ..
        } = self;
        let (venue, decimals, invert) = (*venue, *decimals, *invert);
        let parsed = venue.parse_stamped(text).map_err(Rejected::malformed);
        let (quote, event) = match parsed {
            // A frame that carries no price (an acknowledgement, a heartbeat, a text pong)
            // is a sign of life and nothing more: the same as a protocol Ping.
            Ok(None) => {
                refresh_if_current(tx, *sample_is_current);
                return;
            }
            Ok(Some((quote, event))) => (quote_from_book(quote, decimals, invert), event),
            Err(err) => (Err(err), None),
        };
        match quote {
            Ok((delta, mid)) => {
                let (at, wall) = (Instant::now(), SystemTime::now());
                // Before the sample is sent, so whoever wakes on the sample reads the clock
                // that goes with it.
                let lag = event.map(|event| {
                    self.lag_floor
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .lag(at, wall, event)
                });
                self.connected.priced(PriceClock::new(at, wall, lag));
                *sample_is_current = true;
                if let Some(recorder) = recorder {
                    // The market's own mid, whichever way this lane carries it, so two lanes
                    // on one market record the same series. Through f64: the recorder is a
                    // dashboard series, not a published number. The half-spread is a
                    // fraction of the mid and so the same whichever way the lane is oriented.
                    let market_mid = scaled_to_f64(mid, decimals);
                    let market_mid = if invert { 1.0 / market_mid } else { market_mid };
                    recorder.book(
                        &Recorder::mid_key(venue, symbol),
                        market_mid,
                        scaled_to_f64(delta, decimals),
                    );
                }
                metrics.record(|m| {
                    m.ticks.inc();
                    m.mid.set(scaled_to_f64(mid, decimals));
                    m.delta.set(scaled_to_f64(delta, decimals));
                    m.last_tick.set(
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_secs_f64())
                            .unwrap_or(0.0),
                    );
                    m.sample_current.set(1.0);
                });
                tx.send_replace(Some(PriceSample {
                    delta,
                    mid,
                    at,
                    wall,
                }));
            }
            Err(err) => {
                // The channel keeps the previous sample, but it is now known not to describe
                // the live book, so stop vouching for its freshness.
                *sample_is_current = false;
                metrics.record(|m| {
                    m.rejected(err.kind).inc();
                    m.sample_current.set(0.0);
                });
                tracing::warn!("[{symbol}] {venue}: skipping unusable frame: {err:#}");
            }
        }
    }
}

/// Re-stamps the retained sample as current, if it still is. See the Ping arm above.
fn refresh_if_current(tx: &watch::Sender<Option<PriceSample>>, sample_is_current: bool) {
    if !sample_is_current {
        return;
    }
    tx.send_modify(|sample| {
        if let Some(sample) = sample {
            sample.at = Instant::now();
            sample.wall = SystemTime::now();
        }
    });
}

/// Which bucket a refused bookTicker belongs in. Matches on the error's own text because
/// `quote_from_ticker` reports with `eyre` and its messages are the operator-facing ones;
/// the test below pins every branch so a reworded message cannot silently reclassify.
/// A bookTicker message that could not become a sample: a machine-readable kind for the
/// metric, and the operator message that goes to stderr.
///
/// Modelled on `update::Unusable`, and for the same reason. This used to be
/// `classify_tick_error`, which recovered the kind by substring-matching the rendered error
/// — `text.contains("one-sided")` and friends. Two things were wrong with that. A parse
/// failure embeds the offending value (`invalid decimal number: {s:?}`), so a frame reading
/// `{"b":"one-sided","a":"1.0"}` was counted as a one-sided book. And the label set was
/// coupled to three prose strings in `quote_from_ticker` with only a co-located test holding
/// them together, so rewording an operator message silently reclassified a bucket the
/// dashboard is drawn from.
///
/// The kind now travels with the error, so the two cannot disagree.
#[derive(Debug)]
struct Rejected {
    kind: TickReject,
    why: eyre::Report,
}

impl Rejected {
    /// The frame could not be read at all: not JSON, not a bookTicker, or a side that is not
    /// a number this scaling can hold. Distinct from `Overflow`, which is reserved for *our*
    /// arithmetic running out of room on numbers that did parse.
    fn malformed(why: eyre::Report) -> Self {
        Self {
            kind: TickReject::Malformed,
            why,
        }
    }

    fn of(kind: TickReject, why: eyre::Report) -> Self {
        Self { kind, why }
    }
}

impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.why)
    }
}

/// Turns a venue's quote into the scaled (delta, mid) pair: `mid` is the midpoint of the
/// best bid/ask and `delta` the half-spread as a fraction of it, (ask - bid) / (ask + bid),
/// both scaled by 10^decimals — the [delta, mid] slot pair PropAMM prices fills from.
fn quote_from_book(
    quote: Quote,
    decimals: u32,
    invert: bool,
) -> std::result::Result<(U256, U256), Rejected> {
    let Quote::Book { bid, ask } = quote;
    // An empty side is an empty side, whether the venue spells it "0" (Binance) or as an
    // empty string (OKX, Bitget and MEXC on a market with nothing on one side).
    let bid = if bid.is_empty() { "0" } else { bid.as_str() };
    let ask = if ask.is_empty() { "0" } else { ask.as_str() };
    let bid = parse_decimal_scaled(bid, decimals).map_err(Rejected::malformed)?;
    let ask = parse_decimal_scaled(ask, decimals).map_err(Rejected::malformed)?;
    // Binance reports "0.00000000" for the empty side of a one-sided book; the mid of a
    // one-sided (or empty) book is meaningless and must never be published as a price.
    // Checked before inverting, which divides by both sides.
    if bid.is_zero() || ask.is_zero() {
        return Err(Rejected::of(
            TickReject::OneSided,
            eyre!("one-sided book (bid {bid}, ask {ask}); no meaningful mid"),
        ));
    }
    if bid > ask {
        return Err(Rejected::of(
            TickReject::Crossed,
            eyre!("crossed book (bid {bid} > ask {ask})"),
        ));
    }

    // Compute delta from the original bid and ask; it's invariant under inversion.
    // Checked: U256's + and * panic on overflow, which would kill the feed task (and with
    // it the redial-forever loop) instead of skipping the message like other bad input.
    let overflow = |why: eyre::Report| Rejected::of(TickReject::Overflow, why);
    let sum = bid
        .checked_add(ask)
        .ok_or_else(|| overflow(eyre!("bid {bid} + ask {ask} overflows")))?;
    let delta = (ask - bid)
        .checked_mul(U256::exp10(decimals as usize))
        .ok_or_else(|| overflow(eyre!("spread {} overflows at 10^{decimals}", ask - bid)))?
        / sum;

    // A pair whose tokens sort opposite to its Binance market publishes the reciprocal
    // price. Inverting each side and then taking the midpoint is not the same as
    // inverting the midpoint, and only the former is the true mid of the inverted book.
    // Order is preserved: bid <= ask implies S^2/ask <= S^2/bid.
    let mid = if invert {
        let scale = U256::exp10(decimals as usize);
        let square = scale.checked_mul(scale).ok_or_else(|| {
            overflow(eyre!(
                "10^{decimals} squared overflows a uint256; inverted pairs need \
                 price-decimals <= 38"
            ))
        })?;
        let bid_inv = square / ask;
        let ask_inv = square / bid;
        let sum_inv = bid_inv.checked_add(ask_inv).ok_or_else(|| {
            overflow(eyre!(
                "inverted bid {bid_inv} + inverted ask {ask_inv} overflows"
            ))
        })?;
        sum_inv / 2
    } else {
        sum / 2
    };

    Ok((delta, mid))
}

/// Parses a decimal string like "0.99980000" into value * 10^decimals, truncating
/// fractional digits beyond `decimals`.
pub fn parse_decimal_scaled(s: &str, decimals: u32) -> Result<U256> {
    // U256::MAX has 78 digits, so 78+ decimals can never fit; rejecting up front also
    // bounds the zero-padding allocation below, which grows with `decimals`.
    ensure!(decimals < 78, "{decimals} decimals cannot fit in a uint256");
    // Exponent notation too (`8.31e-6`, `1E3`): JSON allows it and some venues print small
    // prices that way, and a number that is correct but spelled with an exponent must not
    // be refused as malformed. The exponent moves the decimal point; nothing else changes.
    let (mantissa, exponent) = match s.split_once(['e', 'E']) {
        Some((m, e)) => (
            m,
            e.parse::<i32>()
                .map_err(|_| eyre!("invalid exponent in number: {s:?}"))?,
        ),
        None => (s, 0),
    };
    ensure!(
        exponent.abs() <= 100,
        "exponent out of range in number: {s:?}"
    );
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    ensure!(
        !(int_part.is_empty() && frac_part.is_empty()),
        "empty number: {s:?}"
    );
    ensure!(
        int_part
            .chars()
            .chain(frac_part.chars())
            .all(|c| c.is_ascii_digit()),
        "invalid decimal number: {s:?}"
    );
    // All digits in one string, with where the point sits; the exponent shifts it.
    let mut all: String = format!("{int_part}{frac_part}");
    let point = int_part.len() as i64 + exponent as i64;
    let (int_digits, frac_digits) = if point <= 0 {
        all.insert_str(0, &"0".repeat((-point) as usize));
        (String::from("0"), all)
    } else if point as usize >= all.len() {
        all.push_str(&"0".repeat(point as usize - all.len()));
        (all, String::new())
    } else {
        let (i, f) = all.split_at(point as usize);
        (i.to_owned(), f.to_owned())
    };
    let d = decimals as usize;
    let mut digits = int_digits;
    if frac_digits.len() >= d {
        digits.push_str(&frac_digits[..d]);
    } else {
        digits.push_str(&frac_digits);
        digits.push_str(&"0".repeat(d - frac_digits.len()));
    }
    U256::from_dec_str(&digits).map_err(|e| eyre!("number out of range: {s:?}: {e:?}"))
}

/// Renders `value * 10^-decimals` as a decimal string, the inverse of
/// [`parse_decimal_scaled`]. Trailing fractional zeros are trimmed, so a price stored at
/// 18 decimals reads as `0.000249876` rather than as eighteen digits of padding.
pub fn format_scaled(value: U256, decimals: u32) -> String {
    let digits = value.to_string();
    let d = decimals as usize;
    if d == 0 {
        return digits;
    }
    // Left-pad so there is always at least one integer digit before the point.
    let padded = if digits.len() <= d {
        format!("{}{digits}", "0".repeat(d + 1 - digits.len()))
    } else {
        digits
    };
    let (int_part, frac_part) = padded.split_at(padded.len() - d);
    match frac_part.trim_end_matches('0') {
        "" => int_part.to_owned(),
        frac => format!("{int_part}.{frac}"),
    }
}

/// `value * 10^-decimals` as an `f64`, for metric gauges.
///
/// **Lossy, and for display only.** `f64` carries ~15–16 significant digits, which is far
/// more than any dashboard renders but far less than the `U256` arithmetic every publish
/// decision is made in. Nothing that decides what to publish may read this: a future caller
/// reaching for a convenient `f64` price is exactly how a rounding error gets into a quote.
///
/// Goes through the decimal string rather than `U256::as_u128`, which panics above 2^128 —
/// a balance in wei or a mid at 18 decimals both reach that, and a metrics helper must
/// never be able to kill the process it is observing.
pub fn scaled_to_f64(value: U256, decimals: u32) -> f64 {
    format_scaled(value, decimals).parse().unwrap_or(f64::NAN)
}

/// The same price read the other way round, still scaled by `10^decimals`: `1/p` is
/// `10^(2·decimals) / mid`. `None` when there is no such price — a zero mid, or a scale
/// whose square does not fit a uint256.
///
/// This is what makes an inverted lane auditable. `0.000249876` is unverifiable by eye;
/// `≈ 4001.98` is instantly either right or obviously wrong.
pub fn reciprocal_scaled(mid: U256, decimals: u32) -> Option<U256> {
    if mid.is_zero() || decimals >= 78 {
        return None;
    }
    let scale = U256::exp10(decimals as usize);
    Some(scale.checked_mul(scale)? / mid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::SinkExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    /// A feed's metrics, already adopted. Every test here is the only generation on its
    /// label, which is the state `Service::spawn` establishes in production; the unadopted
    /// case has its own test below.
    fn adopted(metrics: &VenueMetrics) -> Gated<VenueMetrics> {
        let switch = Adoption::new();
        switch.adopt();
        Gated::new(metrics.clone(), switch)
    }

    /// A throwaway registry per call: these tests only need somewhere for the feed's
    /// counters to land, and independent registries keep them from interfering.
    fn test_metrics() -> VenueMetrics {
        crate::metrics::Metrics::new()
            .unwrap()
            .for_venue("TEST", "binance")
    }

    /// The two steps the stream loop runs on a Binance frame, composed: the venue's parse
    /// and then the shared scaling. Kept so the scaling tests below read as the frames an
    /// operator sees, and so a parse failure lands in the same `Malformed` bucket it does
    /// in production.
    fn quote_from_ticker(
        text: &str,
        decimals: u32,
        invert: bool,
    ) -> std::result::Result<(U256, U256), Rejected> {
        let quote = crate::venues::binance::parse(text)
            .map_err(Rejected::malformed)?
            .expect("every bookTicker frame is a quote");
        quote_from_book(quote, decimals, invert)
    }

    #[test]
    fn parse_decimal_scaled_handles_binance_formats() {
        let p = |s, d| parse_decimal_scaled(s, d).unwrap();
        assert_eq!(p("0.99980000", 8), U256::from(99_980_000u64));
        assert_eq!(p("1", 8), U256::from(100_000_000u64));
        assert_eq!(p("25.3519", 8), U256::from(2_535_190_000u64));
        assert_eq!(p(".5", 1), U256::from(5));
        // Digits beyond the scale are truncated, not rounded.
        assert_eq!(p("0.123456789", 8), U256::from(12_345_678u64));
        assert_eq!(p("7.9", 0), U256::from(7));
        assert!(parse_decimal_scaled("", 8).is_err());
        assert!(parse_decimal_scaled(".", 8).is_err());
        assert!(parse_decimal_scaled("1,5", 8).is_err());
        assert!(parse_decimal_scaled("-1", 8).is_err());
    }

    #[test]
    fn parse_decimal_scaled_reads_exponent_notation() {
        let p = |s, d| parse_decimal_scaled(s, d).unwrap();
        // What serde_json prints for a small f64, and what a venue may send in JSON.
        assert_eq!(p("8.31e-6", 8), U256::from(831u64));
        assert_eq!(p("8.31E-6", 8), U256::from(831u64));
        assert_eq!(p("1e3", 2), U256::from(100_000u64));
        assert_eq!(p("1.5e2", 0), U256::from(150u64));
        assert_eq!(p("12345e-2", 2), U256::from(12_345u64));
        assert_eq!(p("0.000000000831", 12), p("8.31e-10", 12));
        assert!(parse_decimal_scaled("1e", 8).is_err());
        assert!(parse_decimal_scaled("e5", 8).is_err());
        assert!(parse_decimal_scaled("1e999", 8).is_err());
        // An empty side reads as zero, which the book check then refuses as one-sided.
        assert_eq!(
            quote_from_book(
                Quote::Book {
                    bid: String::new(),
                    ask: "1.0".into()
                },
                8,
                false
            )
            .unwrap_err()
            .kind,
            TickReject::OneSided
        );
    }

    #[test]
    fn parse_decimal_scaled_rejects_unusable_decimals() {
        // 78+ decimals can never fit a uint256; rejecting up front also stops a huge
        // --price-decimals from allocating a `decimals`-sized string per message.
        assert!(parse_decimal_scaled("1.0", 77).is_ok());
        assert!(parse_decimal_scaled("1.0", 78).is_err());
        assert!(parse_decimal_scaled("1.0", u32::MAX).is_err());
    }

    #[test]
    fn quote_from_ticker_computes_mid_and_half_spread() {
        let msg = r#"{"u":1,"s":"USDCUSDT","b":"0.99980000","B":"2.0","a":"1.00000000","A":"3.0"}"#;
        let (delta, mid) = quote_from_ticker(msg, 8, false).unwrap();
        assert_eq!(mid, U256::from(99_990_000u64));
        // (ask - bid) / (ask + bid) = 0.0002 / 1.9998 scaled by 1e8, truncated.
        assert_eq!(delta, U256::from(10_001u64));
        // A book with no spread publishes delta 0.
        let tight = r#"{"b":"2.00000000","a":"2.00000000"}"#;
        assert_eq!(
            quote_from_ticker(tight, 8, false).unwrap(),
            (U256::zero(), U256::from(200_000_000u64))
        );
        assert!(quote_from_ticker("{}", 8, false).is_err());
        assert!(quote_from_ticker("not json", 8, false).is_err());
    }

    #[test]
    fn quote_from_ticker_rejects_one_sided_empty_or_crossed_books() {
        // Binance reports "0.00000000" for the empty side; publishing half the remaining
        // side (or zero) as the mid would put a wrong price on-chain.
        let one_sided = r#"{"b":"0.00000000","a":"0.98000000"}"#;
        assert!(quote_from_ticker(one_sided, 8, false).is_err());
        let empty = r#"{"b":"0.00000000","a":"0.00000000"}"#;
        assert!(quote_from_ticker(empty, 8, false).is_err());
        // A crossed book (bid above ask) is anomalous and would underflow the spread.
        let crossed = r#"{"b":"1.00000000","a":"0.99000000"}"#;
        assert!(quote_from_ticker(crossed, 8, false).is_err());
    }

    #[test]
    fn quote_from_ticker_errors_instead_of_panicking_on_overflow() {
        // A panicking add would kill the feed task and its redial loop for good.
        let max = U256::MAX.to_string();
        let msg = format!(r#"{{"b":"{max}","a":"{max}"}}"#);
        assert!(quote_from_ticker(&msg, 0, false).is_err());
        // A huge spread must not panic the delta scaling either.
        let wide = format!(r#"{{"b":"1","a":"{}"}}"#, U256::MAX / 2);
        assert!(quote_from_ticker(&wide, 30, false).is_err());
    }

    /// The gauges of a lane a reload is *replacing* must survive the swap.
    ///
    /// A reload dials the new feed before stopping the pair it replaces, and both feeds hold
    /// the same label's `VenueMetrics`. Without the adoption gate the outgoing feed always
    /// writes last: the replacement connects and ticks, then the pair it replaced is
    /// stopped, its last receiver drops, and its `tx.closed()` arm sets the same
    /// `last_tick` and `sample_current` to NaN. Nothing writes them again until the next
    /// ticker, so a healthy lane the operator had just reconfigured aged into
    /// PusherFeedStale. `up` is not gated at all any more: each task holds its own
    /// [`Connected`], so the outgoing task's GONE cannot touch the replacement's 1.
    ///
    /// Both halves are asserted here, in the order `Service` drives them: a feed that has
    /// not been adopted yet records nothing, and one that has been let go records nothing on
    /// its way out.
    #[tokio::test]
    async fn a_feed_that_does_not_own_its_pairs_series_does_not_write_to_them() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(mut ws) = accept_async(stream).await {
                        let _ = ws
                            .send(Message::Text(
                                r#"{"u":1,"s":"TESTUSDT","b":"100.00000000","B":"1","a":"100.10000000","A":"1"}"#
                                    .into(),
                            ))
                            .await;
                        std::future::pending::<()>().await;
                    }
                });
            }
        });
        let url = format!("ws://{addr}");
        let metrics = crate::metrics::Metrics::new()
            .unwrap()
            .for_venue("SWAP/TEST", "binance");

        // The lane as it stands before the reload: one pair, quoting, owning the series.
        let outgoing = Adoption::new();
        outgoing.adopt();
        let outgoing_connected = Connected::default();
        let mut outgoing_rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Binance,
            &url,
            "testusdt",
            6,
            false,
            Gated::new(metrics.clone(), outgoing.clone()),
            None,
            outgoing_connected.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), outgoing_rx.changed())
            .await
            .expect("the outgoing feed must connect")
            .unwrap();
        assert_eq!(outgoing_connected.gauge_value(), 1.0, "the lane is quoting");
        let ticks_before = metrics.ticks.get();

        // What a reload does first: dial the replacement, while the pair it replaces is
        // still serving takers.
        let incoming = Adoption::new();
        let incoming_connected = Connected::default();
        let mut incoming_rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Binance,
            &url,
            "testusdt",
            6,
            false,
            Gated::new(metrics.clone(), incoming.clone()),
            None,
            incoming_connected.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), incoming_rx.changed())
            .await
            .expect("the replacement must connect")
            .unwrap();
        assert_eq!(
            metrics.ticks.get(),
            ticks_before,
            "a feed nobody has adopted must not report against the lane still quoting"
        );

        // Then `Service::stop`: the outgoing generation is let go, and only then does its
        // task wind down. `Service::spawn` adopts the replacement immediately after.
        outgoing.release();
        drop(outgoing_rx);
        incoming.adopt();

        // Long enough for the outgoing task to be polled and reach its teardown arm.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            incoming_connected.gauge_value(),
            1.0,
            "the outgoing feed's teardown blinded PusherVenueFeedDown for the replacement"
        );
        assert!(
            outgoing_connected.gauge_value().is_nan(),
            "the outgoing feed is gone, and its own flag is the one that says so"
        );
        assert!(
            !metrics.last_tick.get().is_nan(),
            "and would have aged the replacement into PusherFeedStale too"
        );
    }

    /// A halted pair's feed gauges must not go on reading "connected" for the life of the
    /// process.
    ///
    /// A halt drops the last `watch` receiver, which is how the feed task learns its lane is
    /// finished — and the `tx.closed()` arm used to do nothing, so the inner block was
    /// dropped mid-`read_stream` and `feed_up` kept the `1.0` its last connect wrote. The
    /// dashboard then showed a live feed for a pair whose feed task no longer existed,
    /// PusherFeedDown could never fire, and only the frozen `feed_last_tick` aging out into
    /// PusherFeedStale hinted at it.
    #[tokio::test]
    async fn a_feed_whose_pair_is_gone_stops_claiming_to_be_connected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // One connection that stays open and sends one price, so `feed_up` is genuinely 1.0
        // before the pair goes away — otherwise this test could pass on a gauge that was
        // never set at all.
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await
                && let Ok(mut ws) = accept_async(stream).await
            {
                let _ = ws
                    .send(Message::Text(
                        r#"{"u":1,"s":"TESTUSDT","b":"100.00000000","B":"1","a":"100.10000000","A":"1"}"#
                            .into(),
                    ))
                    .await;
                // Held open: the pair's departure, not the socket's, is what this tests.
                std::future::pending::<()>().await;
            }
        });

        let metrics = crate::metrics::Metrics::new()
            .unwrap()
            .for_venue("HALT/TEST", "binance");
        let connected = Connected::default();
        let mut rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Binance,
            &format!("ws://{addr}"),
            "testusdt",
            6,
            false,
            adopted(&metrics),
            None,
            connected.clone(),
        );
        tokio::time::timeout(Duration::from_secs(5), rx.changed())
            .await
            .expect("the first price must arrive")
            .unwrap();
        assert_eq!(connected.gauge_value(), 1.0, "the feed is connected");

        // What a halt does: the pair is finished, so every receiver goes.
        drop(rx);
        let gone = tokio::time::timeout(Duration::from_secs(5), async {
            while !connected.gauge_value().is_nan() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            gone.is_ok(),
            "venue_feed_up still reads {} after the pair went away",
            connected.gauge_value()
        );
        // All three together, and NaN rather than 0: the values `build_live` gives a pair
        // that has no feed task, which is what this pair now is. See the arm's comment for
        // why a halt must not also raise PusherFeedDown and PusherFeedStale on top of the
        // page the breaker already sent.
        assert!(metrics.last_tick.get().is_nan(), "feed_last_tick");
        assert!(metrics.sample_current.get().is_nan(), "feed_sample_current");
    }

    /// End-to-end over a local mock server: the feed delivers the first connection's
    /// price, then survives the server dropping the connection and delivers a price
    /// from the second connection. Also the metrics' one real drive:
    /// `feed_ticks`/`feed_mid`/`feed_delta`/`feed_last_tick`/`feed_sample_current` tracking
    /// both connections' samples, and `feed_up` reading connected once reconnected — all
    /// through the same `PairMetrics` handle `spawn_feed` records into, not simulated
    /// separately. `feed_up` is not asserted right after the first price: the mock drops
    /// that connection immediately, so whether it has already flipped to 0 by the time this
    /// task is scheduled is a race, not a recording bug.
    #[tokio::test]
    async fn feed_delivers_prices_and_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            ws.send(Message::text(
                r#"{"u":1,"s":"USDCUSDT","b":"0.99980000","B":"1","a":"1.00000000","A":"1"}"#,
            ))
            .await
            .unwrap();
            drop(ws); // kill the connection; the feed must redial
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            ws.send(Message::text(
                r#"{"u":2,"s":"USDCUSDT","b":"2.00000000","B":"1","a":"2.00000000","A":"1"}"#,
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let metrics = test_metrics();
        let connected = Connected::default();
        let mut rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Binance,
            &format!("ws://{addr}/ws"),
            "USDCUSDT",
            8,
            false,
            adopted(&metrics),
            None,
            connected.clone(),
        );
        let first = tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|s| s.is_some()))
            .await
            .expect("no price from first connection")
            .unwrap()
            .unwrap();
        assert_eq!(first.mid, U256::from(99_990_000u64));
        // Not asserting feed_up here: the mock drops the connection right after sending,
        // and that reset can be detected and recorded before this task is even scheduled to
        // check it — feed_up is asserted below instead, once reconnected and stable.
        assert_eq!(metrics.ticks.get(), 1, "one accepted sample so far");
        assert_eq!(metrics.mid.get(), scaled_to_f64(first.mid, 8));
        assert_eq!(metrics.delta.get(), scaled_to_f64(first.delta, 8));
        assert!(
            metrics.last_tick.get() > 0.0,
            "the last-tick timestamp must be stamped"
        );
        assert_eq!(metrics.sample_current.get(), 1.0);

        // RECONNECT_DELAY is 2s, so allow generous time for the redial.
        let second = tokio::time::timeout(
            Duration::from_secs(10),
            rx.wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(200_000_000u64))),
        )
        .await
        .expect("no price after reconnect");
        assert!(second.is_ok());
        // The redial in between must have cleared feed_up on disconnect and set it again
        // on the second connect, not left it stuck at whatever the first connection set.
        assert_eq!(connected.gauge_value(), 1.0, "reconnected after the drop");
        assert_eq!(metrics.ticks.get(), 2, "both connections' samples counted");
        assert_eq!(
            metrics.mid.get(),
            scaled_to_f64(U256::from(200_000_000u64), 8)
        );
    }

    /// A book that goes one-sided and stays there must let its last good sample age out.
    ///
    /// The retained sample is what `ValueSource::current` prices from, and MAX_PRICE_AGE is
    /// the only thing standing between a dead book and a published price. Binance pings
    /// well inside that window, so an unconditional liveness refresh would keep the sample
    /// looking fresh forever. Asserts on the *stamp*, not on the value: the value is
    /// deliberately retained either way, so only `at` can distinguish the two behaviours.
    /// Also asserts `feed_sample_current` settles at 0 once the one-sided ticker has been
    /// processed (not right after the accepted one — the mock queues the two back to back,
    /// so which has landed first is a race, not something a recording bug would explain)
    /// and the `feed_rejected` counter it leaves behind.
    #[tokio::test]
    async fn a_rejected_ticker_stops_pings_from_refreshing_the_retained_sample() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            // A good book, then the same book gone one-sided, then pings forever.
            ws.send(Message::text(
                r#"{"b":"0.99980000","a":"1.00000000"}"#.to_owned(),
            ))
            .await
            .unwrap();
            ws.send(Message::text(
                r#"{"b":"0.00000000","a":"1.00000000"}"#.to_owned(),
            ))
            .await
            .unwrap();
            for _ in 0..20 {
                if ws.send(Message::Ping(Vec::new().into())).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let metrics = test_metrics();
        let mut rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Binance,
            &format!("ws://{addr}/ws"),
            "USDCUSDT",
            8,
            false,
            adopted(&metrics),
            None,
            Default::default(),
        );
        let sample = tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|s| s.is_some()))
            .await
            .expect("no price from the mock")
            .unwrap()
            .unwrap();
        let first_stamp = sample.at;
        // Not asserting feed_sample_current == 1.0 here: the mock queues the one-sided
        // ticker right behind the accepted one with no delay, so the feed task can race
        // ahead and process the reject before this task is even scheduled to check it —
        // the settled value below, after every message has certainly arrived, is not racy.

        // Let every ping arrive. The value must still be the accepted one — a rejected
        // ticker never overwrites — but its stamp must not have moved.
        tokio::time::sleep(Duration::from_millis(600)).await;
        let held = (*rx.borrow()).expect("the last good sample is retained");
        assert_eq!(
            held.mid,
            U256::from(99_990_000u64),
            "a rejected ticker must not overwrite the last good value"
        );
        assert_eq!(
            held.at, first_stamp,
            "pings after a rejected ticker must not refresh the sample's age, or a book \
             stuck one-sided would never age out of MAX_PRICE_AGE"
        );
        assert_eq!(
            metrics.sample_current.get(),
            0.0,
            "a rejected ticker must stop vouching for the retained sample's freshness"
        );
        assert_eq!(
            metrics.rejected(TickReject::OneSided).get(),
            1,
            "the one-sided ticker must be classified and counted"
        );
    }

    /// A polled venue: a local HTTP server answering like MEXC's book ticker. The feed
    /// delivers a sample per fetch, stamps it at the fetch, and reads as up. Also the one
    /// place the shared client's requests are seen from the other side: they name the
    /// pusher, because CoinGecko (the vault's USD prices) and Coinbase answer 403 to a
    /// request without a User-Agent.
    #[tokio::test]
    async fn a_polled_venue_delivers_a_sample_per_fetch() {
        use axum::{Router, extract::Query, routing::get};
        use std::sync::atomic::{AtomicU64, Ordering};
        let hits = Arc::new(AtomicU64::new(0));
        let counted = hits.clone();
        let agent: Arc<std::sync::Mutex<Option<String>>> = Arc::default();
        let seen_agent = agent.clone();
        let app = Router::new().route(
            "/api/v3/ticker/bookTicker",
            get(move |headers: axum::http::HeaderMap,
                      Query(q): Query<std::collections::HashMap<String, String>>| {
                let counted = counted.clone();
                let seen_agent = seen_agent.clone();
                async move {
                    *seen_agent.lock().unwrap() = headers
                        .get("user-agent")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    let n = counted.fetch_add(1, Ordering::SeqCst) + 1;
                    assert_eq!(q["symbol"], "ETHUSDC");
                    // A moving price, so each fetch is a distinguishable sample.
                    format!(
                        r#"{{"symbol":"ETHUSDC","bidPrice":"{n}.0","bidQty":"1","askPrice":"{n}.2","askQty":"1"}}"#
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let metrics = crate::metrics::Metrics::new()
            .unwrap()
            .for_venue("TEST", "mexc");
        let connected = Connected::default();
        let mut rx = spawn_feed(
            Arc::new(http_client()),
            VenueId::Mexc,
            &format!("http://{addr}"),
            "ETHUSDC",
            1,
            false,
            adopted(&metrics),
            None,
            connected.clone(),
        );
        // MEXC polls once a second; two samples prove the loop keeps going.
        let second = tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(21u64))),
        )
        .await
        .expect("no second poll")
        .unwrap()
        .unwrap();
        // (2.0 + 2.2) / 2 = 2.1 at one decimal.
        assert_eq!(second.mid, U256::from(21u64));
        assert!(hits.load(Ordering::SeqCst) >= 2);
        assert_eq!(connected.gauge_value(), 1.0);
        assert_eq!(metrics.sample_current.get(), 1.0);
        assert!(metrics.ticks.get() >= 2);
        let agent = agent.lock().unwrap().clone();
        assert!(
            agent
                .as_deref()
                .is_some_and(|a| a.starts_with("quote-updater/")),
            "the shared client must name the pusher: {agent:?}"
        );
    }

    #[test]
    fn format_scaled_round_trips_through_parse_decimal_scaled() {
        for (text, decimals) in [
            ("0.99980000", 8u32),
            ("4001.98", 18),
            ("0.000249876", 18),
            ("103241.55", 18),
            ("0", 18),
            ("7", 0),
        ] {
            let parsed = parse_decimal_scaled(text, decimals).unwrap();
            // Trailing zeros are trimmed, so compare through the parser rather than
            // against the literal: "0.99980000" renders as "0.9998", same number.
            let rendered = format_scaled(parsed, decimals);
            assert_eq!(
                parse_decimal_scaled(&rendered, decimals).unwrap(),
                parsed,
                "{text} at {decimals} decimals rendered as {rendered}"
            );
        }
    }

    #[test]
    fn format_scaled_renders_the_digits_a_human_checks() {
        // The values `--check`'s price table prints: a mid too small to read as raw
        // integer digits, and its reciprocal.
        assert_eq!(
            format_scaled(parse_decimal_scaled("0.000249876", 18).unwrap(), 18),
            "0.000249876"
        );
        assert_eq!(
            format_scaled(parse_decimal_scaled("4001.98", 18).unwrap(), 18),
            "4001.98"
        );
        // A value below 1 keeps its leading zero, and a whole number keeps no point.
        assert_eq!(format_scaled(U256::from(5u64), 1), "0.5");
        assert_eq!(format_scaled(U256::exp10(18), 18), "1");
        assert_eq!(format_scaled(U256::zero(), 18), "0");
    }

    #[test]
    fn the_reciprocal_reads_an_inverted_lane_in_its_market_direction() {
        // An ETHUSDC book of 4001.98 published on a USDC/WETH lane stores the reciprocal;
        // the reciprocal of that is the number the operator recognises.
        let market = parse_decimal_scaled("4001.98", 18).unwrap();
        let published = reciprocal_scaled(market, 18).expect("a nonzero price inverts");
        let back = reciprocal_scaled(published, 18).expect("and inverts back");
        // Integer division truncates twice, so require the round trip to be close rather
        // than bit-for-bit: within 1e-6 of the price it started from.
        let lost = if back > market {
            back - market
        } else {
            market - back
        };
        assert!(
            lost < U256::exp10(12),
            "inverting twice lost {}, ending at {}",
            format_scaled(lost, 18),
            format_scaled(back, 18)
        );

        // No reciprocal exists for these, and neither may render as a number.
        assert_eq!(reciprocal_scaled(U256::zero(), 18), None);
        // 10^(2*39) overflows a uint256 — the same bound inverted feeds enforce.
        assert!(reciprocal_scaled(U256::one(), 38).is_some());
        assert_eq!(reciprocal_scaled(U256::one(), 39), None);
    }

    #[test]
    fn inverted_quote_preserves_delta_exactly() {
        // ETHUSDC-shaped book: bid 3999, ask 4001.
        let msg = r#"{"b":"3999.00000000","a":"4001.00000000"}"#;
        let (delta_direct, _) = quote_from_ticker(msg, 18, false).unwrap();
        let (delta_inverted, _) = quote_from_ticker(msg, 18, true).unwrap();
        // Inverting a book leaves the half-spread fraction unchanged, bit for bit.
        assert_eq!(delta_direct, delta_inverted);
    }

    #[test]
    fn inverted_mid_is_the_midpoint_of_the_inverted_book() {
        let msg = r#"{"b":"3999.00000000","a":"4001.00000000"}"#;
        let (_, mid_inverted) = quote_from_ticker(msg, 18, true).unwrap();

        let scale = U256::exp10(18);
        let square = scale * scale;
        let bid = U256::from(3999u64) * scale;
        let ask = U256::from(4001u64) * scale;
        // The correct value: invert each side, then take the midpoint.
        assert_eq!(mid_inverted, (square / ask + square / bid) / 2);

        // Not the same as inverting the direct mid, which is why the order matters.
        let (_, mid_direct) = quote_from_ticker(msg, 18, false).unwrap();
        assert_ne!(mid_inverted, square / mid_direct);
    }

    #[test]
    fn inverting_rejects_decimals_that_overflow_the_square() {
        let msg = r#"{"b":"1.0","a":"1.0"}"#;
        // 10^(2*38) fits a uint256; 10^(2*39) does not.
        assert!(quote_from_ticker(msg, 38, true).is_ok());
        assert!(quote_from_ticker(msg, 39, true).is_err());
        // Without inversion the old, looser bound still holds.
        assert!(quote_from_ticker(msg, 39, false).is_ok());
    }

    #[test]
    fn inverting_still_rejects_one_sided_and_crossed_books() {
        // The zero check must run before the division, or inversion divides by zero.
        assert!(quote_from_ticker(r#"{"b":"0.0","a":"4001.0"}"#, 18, true).is_err());
        assert!(quote_from_ticker(r#"{"b":"4001.0","a":"3999.0"}"#, 18, true).is_err());
    }

    #[test]
    fn scaled_to_f64_reads_a_whole_number() {
        assert_eq!(
            scaled_to_f64(U256::from(4000u64) * U256::exp10(18), 18),
            4000.0
        );
    }

    #[test]
    fn scaled_to_f64_reads_a_small_fraction() {
        // 0.00025 at 18 decimals: an inverted WETH/USDC lane.
        let mid = U256::exp10(18) / U256::from(4000u64);
        let got = scaled_to_f64(mid, 18);
        assert!((got - 0.00025).abs() < 1e-12, "got {got}");
    }

    #[test]
    fn scaled_to_f64_is_zero_at_zero() {
        assert_eq!(scaled_to_f64(U256::zero(), 18), 0.0);
    }

    #[test]
    fn scaled_to_f64_survives_a_huge_value() {
        // Must not panic or return NaN on a value far past f64's integer precision.
        let got = scaled_to_f64(U256::MAX, 18);
        assert!(got.is_finite() && got > 0.0, "got {got}");
    }

    #[test]
    fn scaled_to_f64_handles_zero_decimals() {
        assert_eq!(scaled_to_f64(U256::from(42u64), 0), 42.0);
    }

    #[tokio::test]
    async fn a_rejected_ticker_is_counted_and_stops_vouching_for_the_sample() {
        let m = crate::metrics::Metrics::new().unwrap();
        let pm = m.for_venue("USDC/USDT", "binance");
        // The classification the stream loop applies, exercised directly: quote_from_ticker's
        // error text is what decides the bucket, and it must not drift from the enum. Every
        // case here is a message quote_from_ticker genuinely refuses, not a simulated error
        // string, so a reworded message fails this test instead of only misclassifying quietly
        // in production.
        let max = U256::MAX.to_string();
        for (text, decimals, expect) in [
            (
                r#"{"b":"0","a":"1.0"}"#.to_owned(),
                18,
                TickReject::OneSided,
            ),
            (
                r#"{"b":"2.0","a":"1.0"}"#.to_owned(),
                18,
                TickReject::Crossed,
            ),
            ("not json".to_owned(), 18, TickReject::Malformed),
            // The case a substring classifier got wrong: `parse_decimal_scaled` embeds the
            // offending value in its message, so a side whose text happens to read
            // "one-sided" was counted as a one-sided book. The kind travels with the error
            // now, so the frame's contents cannot vote on its own classification.
            (
                r#"{"b":"one-sided","a":"1.0"}"#.to_owned(),
                18,
                TickReject::Malformed,
            ),
            // Likewise a value containing the crossed-book wording.
            (
                r#"{"b":"crossed book","a":"1.0"}"#.to_owned(),
                18,
                TickReject::Malformed,
            ),
            // bid + ask overflows a U256 at 0 decimals — the same case
            // `quote_from_ticker_errors_instead_of_panicking_on_overflow` exercises.
            (
                format!(r#"{{"b":"{max}","a":"{max}"}}"#),
                0,
                TickReject::Overflow,
            ),
        ] {
            let err = quote_from_ticker(&text, decimals, false).unwrap_err();
            assert_eq!(err.kind, expect, "for {text}");
        }
        pm.rejected(TickReject::OneSided).inc();
        assert!(
            m.render()
                .unwrap()
                .contains(r#"pair="USDC/USDT",reason="one_sided",venue="binance"} 1"#)
        );
    }
}
