//! `us_market_hours`: a quote guard that withdraws a stock pair's quote outside the US regular
//! session, 09:30–16:00 New York time on trading days (13:00 on early-close days).
//!
//! The stock tokens trade on chain around the clock, but the price behind our quote, and the
//! venue we restock and hedge on, only move in that window. Outside it a quote would offer the
//! last close to whoever knows where the next open will be.
//!
//! ```toml
//! [[pairs.guards]]
//! kind              = "us_market_hours"
//! open_delay_secs   = 60     # start this long after 09:30, once the opening auction settles
//! close_margin_secs = 60     # stop this long before the close
//! holidays          = ["2026-11-26", "2026-12-25"]   # NYSE full closures
//! early_closes      = ["2026-11-27", "2026-12-24"]   # NYSE 13:00 closes
//! listed_through    = "2026-12-31"                   # the last day the lists cover
//! ```
//!
//! The calendar is the operator's to keep, not this crate's: NYSE publishes it a few years
//! ahead, and a list in the file is one an operator can read and fix without a release. Both
//! lists are required, so forgetting them fails the file instead of quoting through a holiday,
//! and so is `listed_through`: past it the guard cannot tell a holiday from a trading day, so
//! it withdraws under its own reason, `calendar_ended`, rather than quote through the next
//! unlisted holiday. From a month before, the guard warns once a New York day, from its
//! build and from a process that keeps running alike, and every tick publishes the days left
//! as the `calendar_days_left` diagnostic, for an alert that does not depend on the logs.
//!
//! New York time is computed here rather than with a timezone crate: US daylight saving runs
//! from 02:00 on the second Sunday of March to 02:00 on the first Sunday of November (the rule
//! since 2007), and that is the only rule this guard needs.
//!
//! The guard reads the wall clock on every tick. The quote it lets through is for the next
//! block, up to one block time later, which is what `close_margin_secs` covers.

use std::{
    collections::BTreeSet,
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use quote_updater::{eyre, prelude::*};
use serde::Deserialize;

/// The `[[pairs.guards]]` stanza for `kind = "us_market_hours"`.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UsMarketHoursCfg {
    /// Seconds after 09:30 before the first quote (default 60).
    #[serde(default = "default_margin_secs")]
    pub open_delay_secs: u64,
    /// Seconds before the close the last quote stops (default 60).
    #[serde(default = "default_margin_secs")]
    pub close_margin_secs: u64,
    /// Exchange holidays, `YYYY-MM-DD`. Required, so it is written down, even if empty.
    pub holidays: Vec<String>,
    /// Days the exchange closes at 13:00, `YYYY-MM-DD`. Required, like `holidays`.
    pub early_closes: Vec<String>,
    /// The last day the two lists cover, `YYYY-MM-DD`. Required: past it the guard withdraws.
    pub listed_through: String,
}

fn default_margin_secs() -> u64 {
    60
}

impl UsMarketHoursCfg {
    fn calendar(&self) -> eyre::Result<Calendar> {
        Calendar::new(
            &self.holidays,
            &self.early_closes,
            &self.listed_through,
            self.open_delay_secs,
            self.close_margin_secs,
        )
        .map_err(|err| eyre::eyre!(err))
    }
}

/// The `us_market_hours` guard: withdraws while [`Calendar::session`] says closed.
pub struct UsMarketHours {
    calendar: Calendar,
    closed: RefusalHandle,
    /// Past the calendar's last listed day.
    ended: RefusalHandle,
    /// How many New York days the calendar has left, set every tick.
    days_left: DiagHandle,
    /// The New York day the guard last warned that the calendar is ending.
    warned_on: Option<i64>,
    /// The pair, as its log lines name it.
    label: String,
    /// Seconds since the epoch now; the wall clock in the run, a fixed instant in a test.
    clock: fn() -> i64,
}

impl UsMarketHours {
    /// That the calendar is ending or has ended, the first time it is asked on each New
    /// York day from a month before; `None` otherwise.
    fn daily_warning(&mut self, unix: i64) -> Option<String> {
        let today = new_york_day(unix);
        if self.warned_on == Some(today) {
            return None;
        }
        let warning = end_warning(self.calendar.days_left(unix), self.calendar.through)?;
        self.warned_on = Some(today);
        Some(warning)
    }
}

impl QuoteGuard for UsMarketHours {
    fn check(&mut self, _candidate: &Candidate<'_>, out: &mut Diagnostics) -> Gate {
        let now = (self.clock)();
        out.set(&self.days_left, self.calendar.days_left(now) as f64);
        if let Some(warning) = self.daily_warning(now) {
            tracing::warn!("[{}] us_market_hours: {warning}", self.label);
        }
        match self.calendar.session(now) {
            Ok(()) => Gate::Allow,
            Err(Closed::Session(why)) => {
                Gate::Withdraw(Refusal::new(&self.closed, format!("market closed: {why}")))
            }
            Err(Closed::Unlisted(why)) => Gate::Withdraw(Refusal::new(&self.ended, why)),
        }
    }
}

fn wall_clock() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() as i64)
}

/// Registered as `us_market_hours`.
pub struct UsMarketHoursFactory;

impl QuoteGuardFactory for UsMarketHoursFactory {
    type Config = UsMarketHoursCfg;
    type Guard = UsMarketHours;

    fn validate(&self, cfg: &UsMarketHoursCfg, _pair: &PairShape) -> eyre::Result<()> {
        cfg.calendar().map(drop)
    }

    fn summary(&self, cfg: &UsMarketHoursCfg) -> String {
        format!(
            "{} holidays, {} early closes through {}, from +{}s to -{}s",
            cfg.holidays.len(),
            cfg.early_closes.len(),
            cfg.listed_through,
            cfg.open_delay_secs,
            cfg.close_margin_secs
        )
    }

    fn build<'a>(
        &'a self,
        cfg: &'a UsMarketHoursCfg,
        ctx: &'a mut BuildCtx,
    ) -> BoxFuture<'a, eyre::Result<UsMarketHours>> {
        Box::pin(async move {
            let mut guard = UsMarketHours {
                calendar: cfg.calendar()?,
                closed: ctx.refusal("market_closed")?,
                ended: ctx.refusal("calendar_ended")?,
                days_left: ctx.diagnostic("calendar_days_left")?,
                warned_on: None,
                label: ctx.pair().label.clone(),
                clock: wall_clock,
            };
            // Said at build too, so `--check` shows it.
            if let Some(warning) = guard.daily_warning(wall_clock()) {
                tracing::warn!("[{}] us_market_hours: {warning}", guard.label);
            }
            Ok(guard)
        })
    }
}

/// A calendar date, as the config writes it: `2026-11-26`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

impl FromStr for Date {
    type Err = String;

    fn from_str(s: &str) -> Result<Date, String> {
        let bad = || format!("`{s}` is not a date written YYYY-MM-DD");
        let bytes = s.as_bytes();
        if !s.is_ascii() || bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return Err(bad());
        }
        let number = |range: std::ops::Range<usize>| -> Result<u32, String> {
            let part = &s[range];
            if !part.bytes().all(|c| c.is_ascii_digit()) {
                return Err(bad());
            }
            part.parse().map_err(|_| bad())
        };
        let (year, month, day) = (number(0..4)? as i32, number(5..7)?, number(8..10)?);
        if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
            return Err(format!("`{s}` is not a day of the calendar"));
        }
        Ok(Date { year, month, day })
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// How close to its end a calendar has its build warn.
const WARN_DAYS_BEFORE_THE_END: i64 = 30;

/// What the build says about a calendar `left` New York days from its last listed day,
/// `through`: nothing until a month before, then that it is ending, or that it has ended.
fn end_warning(left: i64, through: Date) -> Option<String> {
    if left >= WARN_DAYS_BEFORE_THE_END {
        return None;
    }
    Some(if left < 0 {
        format!(
            "the calendar ended {through}: the guard withdraws every quote (calendar_ended) \
             until the next year's holidays and early closes are listed and listed_through is \
             moved"
        )
    } else {
        format!(
            "the calendar ends {through} ({left} days from today): list the next year's \
             holidays and early closes and move listed_through, or the guard withdraws from \
             the day after"
        )
    })
}

/// The trading calendar the guard judges by, and the margins inside the session it keeps.
#[derive(Clone, Debug)]
pub struct Calendar {
    holidays: BTreeSet<Date>,
    early_closes: BTreeSet<Date>,
    /// The last day the lists cover.
    through: Date,
    open_delay_secs: i64,
    close_margin_secs: i64,
}

/// Why the guard withdraws at an instant, for the withdrawn quote's log line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Closed {
    /// Outside the session: a weekend, a holiday, before the open or after the close.
    Session(String),
    /// Past the calendar's last listed day, where a holiday would read as a trading day.
    Unlisted(String),
}

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Closed::Session(why) | Closed::Unlisted(why) => f.write_str(why),
        }
    }
}

impl Calendar {
    /// A calendar from the config's lists. Refuses a date that is not a weekday (the exchange
    /// is closed then anyway, so a weekend date is a typo), a date on both lists, a date past
    /// `listed_through`, and margins that leave no window on a short day.
    pub fn new(
        holidays: &[String],
        early_closes: &[String],
        listed_through: &str,
        open_delay_secs: u64,
        close_margin_secs: u64,
    ) -> Result<Calendar, String> {
        let through: Date = listed_through
            .parse()
            .map_err(|err| format!("listed_through: {err}"))?;
        let weekdays_only = |dates: &[String]| -> Result<BTreeSet<Date>, String> {
            dates
                .iter()
                .map(|written| {
                    let date: Date = written.parse()?;
                    let weekday = weekday(days_from_civil(date.year, date.month, date.day));
                    if weekday == 0 || weekday == 6 {
                        return Err(format!(
                            "{date} is a {}: the exchange is closed on weekends anyway, so it \
                             can only be a typo",
                            WEEKDAYS[weekday as usize]
                        ));
                    }
                    Ok(date)
                })
                .collect()
        };
        let holidays = weekdays_only(holidays)?;
        let early_closes = weekdays_only(early_closes)?;
        if let Some(date) = holidays.intersection(&early_closes).next() {
            return Err(format!(
                "{date} is on both lists, holidays and early_closes: say which it is"
            ));
        }
        if let Some(date) = holidays.union(&early_closes).find(|date| **date > through) {
            return Err(format!(
                "{date} is listed but listed_through is {through}: move listed_through to the \
                 last day the lists cover"
            ));
        }
        let shortest = (EARLY_CLOSE - OPEN) as u64;
        // Saturating: wrapped, two huge margins would add up to none.
        let margins = open_delay_secs.saturating_add(close_margin_secs);
        if margins >= shortest {
            return Err(format!(
                "open_delay_secs + close_margin_secs is {margins}s, which leaves no session on \
                 an early-close day (09:30 to 13:00, {shortest}s)"
            ));
        }
        Ok(Calendar {
            holidays,
            early_closes,
            through,
            open_delay_secs: open_delay_secs as i64,
            close_margin_secs: close_margin_secs as i64,
        })
    }

    /// `Ok` while quoting is allowed at `unix` (seconds since the epoch, UTC), or why not.
    pub fn session(&self, unix: i64) -> Result<(), Closed> {
        let local = unix + new_york_offset(unix);
        let days = local.div_euclid(86_400);
        let secs = local.rem_euclid(86_400);
        let date = civil_from_days(days);
        let weekday = weekday(days);
        let at = format!(
            "{} {date} {:02}:{:02} New York",
            WEEKDAYS[weekday as usize],
            secs / 3_600,
            secs / 60 % 60
        );
        // First, weekends included: a calendar that ran out has to be heard, not just
        // noticed on the next trading day.
        if date > self.through {
            return Err(Closed::Unlisted(format!(
                "the calendar ends {} ({at}): list the next year's holidays and early closes \
                 and move listed_through",
                self.through
            )));
        }
        if weekday == 0 || weekday == 6 {
            return Err(Closed::Session(format!("weekend ({at})")));
        }
        if self.holidays.contains(&date) {
            return Err(Closed::Session(format!("holiday ({at})")));
        }
        let close = if self.early_closes.contains(&date) {
            EARLY_CLOSE
        } else {
            CLOSE
        };
        let from = OPEN + self.open_delay_secs;
        let until = close - self.close_margin_secs;
        if secs < from {
            return Err(Closed::Session(format!(
                "before the open ({at}; quoting from {})",
                clock(from)
            )));
        }
        if secs >= until {
            return Err(Closed::Session(format!(
                "after the close ({at}; quoting stopped at {})",
                clock(until)
            )));
        }
        Ok(())
    }

    /// Whole days from the New York date at `unix` to the last listed one: 0 on that day,
    /// negative past it.
    pub fn days_left(&self, unix: i64) -> i64 {
        days_from_civil(self.through.year, self.through.month, self.through.day)
            - new_york_day(unix)
    }
}

/// The New York date at `unix`, as days since 1970-01-01.
fn new_york_day(unix: i64) -> i64 {
    (unix + new_york_offset(unix)).div_euclid(86_400)
}

/// New York's offset from UTC at a UTC instant, in seconds: EDT (-4h) from 07:00 UTC on the
/// second Sunday of March to 06:00 UTC on the first Sunday of November, EST (-5h) otherwise.
pub fn new_york_offset(unix: i64) -> i64 {
    // The UTC year is New York's at both switches: neither is near New Year.
    let year = civil_from_days(unix.div_euclid(86_400)).year;
    let starts = nth_sunday(year, 3, 2) * 86_400 + 7 * 3_600;
    let ends = nth_sunday(year, 11, 1) * 86_400 + 6 * 3_600;
    if (starts..ends).contains(&unix) {
        -4 * 3_600
    } else {
        -5 * 3_600
    }
}

/// The regular session, in seconds after New York midnight.
const OPEN: i64 = 9 * 3_600 + 30 * 60;
const CLOSE: i64 = 16 * 3_600;
const EARLY_CLOSE: i64 = 13 * 3_600;

const WEEKDAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

/// `HH:MM:SS` of seconds after midnight.
fn clock(secs: i64) -> String {
    format!("{:02}:{:02}:{:02}", secs / 3_600, secs / 60 % 60, secs % 60)
}

/// The day of the week of `days` after 1970-01-01, a Thursday: 0 is Sunday, 6 Saturday.
fn weekday(days: i64) -> u32 {
    (days + 4).rem_euclid(7) as u32
}

/// Days since 1970-01-01 of the `n`th Sunday (1-based) of a month.
fn nth_sunday(year: i32, month: u32, n: i64) -> i64 {
    let first = days_from_civil(year, month, 1);
    let to_sunday = i64::from((7 - weekday(first)) % 7);
    first + to_sunday + 7 * (n - 1)
}

fn is_leap(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's `days_from_civil`).
pub(crate) fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The date `days` after 1970-01-01 (Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> Date {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * shifted_month + 2) / 5 + 1) as u32;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    } as u32;
    let year = (year_of_era + era * 400 + i64::from(month <= 2)) as i32;
    Date { year, month, day }
}

#[cfg(test)]
mod tests {
    use quote_updater::testing;

    use super::*;

    fn stanza() -> UsMarketHoursCfg {
        UsMarketHoursCfg {
            open_delay_secs: 60,
            close_margin_secs: 60,
            holidays: vec!["2026-11-26".to_owned()],
            early_closes: vec!["2026-11-27".to_owned()],
            listed_through: "2026-12-31".to_owned(),
        }
    }

    /// The guard as the run builds it, its clock stopped at `unix`.
    async fn built_at(unix: fn() -> i64) -> (UsMarketHours, BuildCtx) {
        let mut ctx = testing::build_ctx(&testing::pair());
        let mut guard = UsMarketHoursFactory
            .build(&stanza(), &mut ctx)
            .await
            .unwrap();
        guard.clock = unix;
        (guard, ctx)
    }

    /// `guard`'s verdict on a tick, and the diagnostics it set.
    fn check(guard: &mut UsMarketHours, ctx: &BuildCtx) -> (Gate, Diagnostics) {
        let pair = testing::pair();
        let tick = testing::tick(&pair, None);
        let priced = PricerOutput::new(U256::exp10(15), U256::exp10(18) * U256::from(250));
        let seen = testing::diagnostics(ctx);
        let mut out = testing::diagnostics(ctx);
        let gate = guard.check(&testing::candidate(&tick, Ok(&priced), &seen), &mut out);
        (gate, out)
    }

    /// The guard's verdict on a tick, its clock stopped at `unix`.
    async fn verdict_at(unix: fn() -> i64) -> Gate {
        let (mut guard, ctx) = built_at(unix).await;
        check(&mut guard, &ctx).0
    }

    #[test]
    fn the_alert_on_the_days_left_fires_when_the_guard_starts_warning() {
        // PusherStockCalendarEnding stands in for this guard's log line: both a month out.
        assert_eq!(WARN_DAYS_BEFORE_THE_END, 30);
        let rules = include_str!("../../../deploy/alerts/quote-updater.rules.yml");
        assert!(
            rules.contains(
                r#"quote_updater_diagnostic{kind="us_market_hours",name="calendar_days_left"} < 30"#
            ),
            "the calendar alert's threshold drifted from the guard's"
        );
    }

    #[tokio::test]
    async fn every_tick_says_how_many_days_the_calendar_has_left() {
        // Wednesday 2026-12-02, 10:00 EST: 29 days to 2026-12-31.
        let (mut guard, ctx) = built_at(|| utc(2026, 12, 2, 15, 0, 0)).await;
        let (_, out) = check(&mut guard, &ctx);
        assert_eq!(out.get(&guard.days_left), Some(29.0));
    }

    #[tokio::test]
    async fn a_running_guard_warns_once_a_day_from_a_month_before_its_calendar_ends() {
        let (mut guard, _) = built_at(|| utc(2026, 10, 8, 14, 0, 0)).await;
        assert_eq!(
            guard.daily_warning(utc(2026, 11, 15, 15, 0, 0)),
            None,
            "46 days left"
        );
        assert!(guard.daily_warning(utc(2026, 12, 2, 15, 0, 0)).is_some());
        assert_eq!(
            guard.daily_warning(utc(2026, 12, 2, 20, 0, 0)),
            None,
            "the same day"
        );
        // 00:30 UTC on the 3rd is still the 2nd in New York.
        assert_eq!(guard.daily_warning(utc(2026, 12, 3, 0, 30, 0)), None);
        assert!(
            guard.daily_warning(utc(2026, 12, 3, 15, 0, 0)).is_some(),
            "the next day"
        );
    }

    #[tokio::test]
    async fn the_guard_allows_a_quote_in_session() {
        // Thursday 2026-10-08, 10:00 EDT.
        assert_eq!(verdict_at(|| utc(2026, 10, 8, 14, 0, 0)).await, Gate::Allow);
    }

    #[tokio::test]
    async fn the_guard_withdraws_out_of_session_under_its_own_reason() {
        // Saturday.
        let Gate::Withdraw(refusal) = verdict_at(|| utc(2026, 10, 10, 15, 0, 0)).await else {
            panic!("a weekend quote was not withdrawn");
        };
        assert_eq!(refusal.reason(), "market_closed");
        assert!(
            refusal.message().contains("weekend"),
            "{}",
            refusal.message()
        );
    }

    #[test]
    fn the_stanza_is_checked_before_anything_is_built() {
        let pair = testing::pair();
        assert!(UsMarketHoursFactory.validate(&stanza(), &pair).is_ok());
        let typo = UsMarketHoursCfg {
            holidays: vec!["2026-11-31".to_owned()],
            ..stanza()
        };
        assert!(UsMarketHoursFactory.validate(&typo, &pair).is_err());
        assert_eq!(
            UsMarketHoursFactory.summary(&stanza()),
            "1 holidays, 1 early closes through 2026-12-31, from +60s to -60s"
        );
    }

    #[tokio::test]
    async fn past_its_last_listed_day_the_guard_withdraws_under_its_own_reason() {
        // Monday 2027-01-18, 10:00 EST: MLK Day, which the stanza (through 2026-12-31) does
        // not list. A trading day by every rule the guard has, so only the end of the
        // calendar can stop it.
        let Gate::Withdraw(refusal) = verdict_at(|| utc(2027, 1, 18, 15, 0, 0)).await else {
            panic!("a day past the calendar was quoted");
        };
        assert_eq!(refusal.reason(), "calendar_ended");
        assert!(
            refusal.message().contains("2026-12-31"),
            "{}",
            refusal.message()
        );
    }

    #[test]
    fn the_stanza_refuses_unknown_keys_and_requires_both_lists_and_their_end() {
        testing::assert_rejects_unknown_fields::<UsMarketHoursCfg>(
            "holidays = []\nearly_closes = []\nlisted_through = \"2026-12-31\"",
        );
        testing::assert_rejects_unknown_fields::<UsMarketHoursCfg>(
            "holidays = []\nearly_closes = []\nlisted_through = \"2026-12-31\"\n\
             open_delay_secs = 0\nclose_margin_secs = 30",
        );
        // Without its end, a calendar would be read as covering every year to come.
        let unbounded = serde_json::json!({"holidays": [], "early_closes": []});
        assert!(serde_json::from_value::<UsMarketHoursCfg>(unbounded).is_err());
    }

    #[test]
    fn dates_round_trip_through_day_numbers() {
        // Every 37th day across roughly ±2,200 years, and the epoch's own weekday.
        for days in (-800_000..800_000).step_by(37) {
            let date = civil_from_days(days);
            assert_eq!(
                days_from_civil(date.year, date.month, date.day),
                days,
                "{date}"
            );
        }
        assert_eq!(weekday(days_from_civil(2026, 10, 8)), 4, "a Thursday");
    }

    /// Seconds since the epoch of a UTC wall time.
    fn utc(year: i32, month: u32, day: u32, hour: i64, min: i64, sec: i64) -> i64 {
        days_from_civil(year, month, day) * 86_400 + hour * 3_600 + min * 60 + sec
    }

    const THROUGH: &str = "2026-12-31";

    fn calendar() -> Calendar {
        Calendar::new(
            &["2026-11-26".to_owned(), "2026-12-25".to_owned()],
            &["2026-11-27".to_owned(), "2026-12-24".to_owned()],
            THROUGH,
            60,
            60,
        )
        .unwrap()
    }

    #[test]
    fn dates_parse_only_in_the_one_spelling_the_config_uses() {
        assert_eq!(
            "2026-02-03".parse::<Date>(),
            Ok(Date {
                year: 2026,
                month: 2,
                day: 3
            })
        );
        for bad in [
            "2026-2-03",
            "2026-02-3",
            " 2026-02-03",
            "2026/02/03",
            "2026-02-30",
            "2026-13-01",
            "2026-00-10",
            "2026-02-00",
            "",
        ] {
            assert!(bad.parse::<Date>().is_err(), "{bad:?} parsed");
        }
        assert!("2028-02-29".parse::<Date>().is_ok(), "2028 is a leap year");
        assert!(
            "2026-02-29".parse::<Date>().is_err(),
            "2026 is not a leap year"
        );
    }

    #[test]
    fn daylight_saving_starts_at_two_am_on_the_second_sunday_of_march() {
        // 2026-03-08 is the second Sunday; 02:00 EST is 07:00 UTC.
        assert_eq!(new_york_offset(utc(2026, 3, 8, 6, 59, 59)), -5 * 3_600);
        assert_eq!(new_york_offset(utc(2026, 3, 8, 7, 0, 0)), -4 * 3_600);
    }

    #[test]
    fn daylight_saving_ends_at_two_am_on_the_first_sunday_of_november() {
        // 2026-11-01 is the first Sunday; 02:00 EDT is 06:00 UTC.
        assert_eq!(new_york_offset(utc(2026, 11, 1, 5, 59, 59)), -4 * 3_600);
        assert_eq!(new_york_offset(utc(2026, 11, 1, 6, 0, 0)), -5 * 3_600);
    }

    #[test]
    fn daylight_saving_switches_on_the_rules_sundays_whatever_weekday_the_month_starts_on() {
        // In 2026 both months start on a Sunday, which a rule that ignored the weekday would
        // also get right. These start on a Monday, a Wednesday and a Friday.
        for (year, march, november) in [(2027, 14, 7), (2028, 12, 5), (2030, 10, 3)] {
            let at = format!("{year}: March {march}, November {november}");
            assert_eq!(
                new_york_offset(utc(year, 3, march, 6, 59, 59)),
                -5 * 3_600,
                "{at}"
            );
            assert_eq!(
                new_york_offset(utc(year, 3, march, 7, 0, 0)),
                -4 * 3_600,
                "{at}"
            );
            assert_eq!(
                new_york_offset(utc(year, 11, november, 5, 59, 59)),
                -4 * 3_600,
                "{at}"
            );
            assert_eq!(
                new_york_offset(utc(year, 11, november, 6, 0, 0)),
                -5 * 3_600,
                "{at}"
            );
        }
    }

    #[test]
    fn the_open_moves_an_hour_in_utc_across_each_switch() {
        let calendar = Calendar::new(&[], &[], "2027-12-31", 60, 60).unwrap();
        let opens = |year, month, day, hour| {
            (
                calendar
                    .session(utc(year, month, day, hour, 30, 59))
                    .is_ok(),
                calendar.session(utc(year, month, day, hour, 31, 0)).is_ok(),
            )
        };
        // 09:31 New York, a minute after the bell: 14:31Z in EST, 13:31Z in EDT.
        assert_eq!(opens(2027, 3, 12, 14), (false, true), "Friday before, EST");
        assert_eq!(opens(2027, 3, 15, 13), (false, true), "Monday after, EDT");
        assert_eq!(opens(2027, 11, 5, 13), (false, true), "Friday before, EDT");
        assert_eq!(opens(2027, 11, 8, 14), (false, true), "Monday after, EST");
    }

    #[test]
    fn the_build_warns_a_month_before_the_calendar_ends_and_says_so_once_it_has() {
        let through: Date = "2027-12-31".parse().unwrap();
        assert_eq!(end_warning(30, through), None);
        let ending = end_warning(29, through).unwrap();
        assert!(ending.contains("ends 2027-12-31 (29 days"), "{ending}");
        assert!(end_warning(0, through).unwrap().contains("(0 days"));
        let ended = end_warning(-5, through).unwrap();
        assert!(ended.contains("ended 2027-12-31"), "{ended}");
        assert!(!ended.contains("days from today"), "{ended}");
    }

    #[test]
    fn a_weekday_mid_session_is_open() {
        // Thursday 2026-10-08, 10:00 EDT.
        assert_eq!(calendar().session(utc(2026, 10, 8, 14, 0, 0)), Ok(()));
    }

    #[test]
    fn the_session_opens_after_the_open_delay() {
        let cal = calendar();
        // 09:30:59 EDT is inside the 60s delay; 09:31:00 is the first second quoted.
        assert!(cal.session(utc(2026, 10, 8, 13, 30, 59)).is_err());
        assert_eq!(cal.session(utc(2026, 10, 8, 13, 31, 0)), Ok(()));
        assert!(
            cal.session(utc(2026, 10, 8, 12, 0, 0)).is_err(),
            "08:00 EDT"
        );
    }

    #[test]
    fn the_session_closes_the_margin_before_the_bell() {
        let cal = calendar();
        // 15:58:59 EDT is quoted; from 15:59:00 the 60s margin withdraws.
        assert_eq!(cal.session(utc(2026, 10, 8, 19, 58, 59)), Ok(()));
        assert!(cal.session(utc(2026, 10, 8, 19, 59, 0)).is_err());
        assert!(
            cal.session(utc(2026, 10, 8, 21, 0, 0)).is_err(),
            "17:00 EDT"
        );
    }

    #[test]
    fn standard_time_shifts_the_session_an_hour_later_in_utc() {
        let cal = calendar();
        // Monday 2026-11-02, the first trading day on EST: 09:31 EST is 14:31 UTC.
        assert!(
            cal.session(utc(2026, 11, 2, 13, 31, 0)).is_err(),
            "08:31 EST"
        );
        assert_eq!(cal.session(utc(2026, 11, 2, 14, 31, 0)), Ok(()));
        // Friday 2026-03-06, the last trading day on EST before the switch.
        assert_eq!(cal.session(utc(2026, 3, 6, 14, 31, 0)), Ok(()));
        // Monday 2026-03-09, on EDT: 09:31 EDT is 13:31 UTC.
        assert_eq!(cal.session(utc(2026, 3, 9, 13, 31, 0)), Ok(()));
    }

    #[test]
    fn weekends_are_closed_whatever_the_time() {
        let cal = calendar();
        let saturday = cal.session(utc(2026, 10, 10, 15, 0, 0)).unwrap_err();
        assert!(saturday.to_string().contains("weekend"), "{saturday}");
        assert!(cal.session(utc(2026, 10, 11, 15, 0, 0)).is_err(), "Sunday");
    }

    #[test]
    fn a_holiday_is_closed_all_day() {
        // Thanksgiving, 11:00 EST.
        let closed = calendar().session(utc(2026, 11, 26, 16, 0, 0)).unwrap_err();
        assert!(closed.to_string().contains("holiday"), "{closed}");
    }

    #[test]
    fn the_calendar_ends_on_its_last_listed_day() {
        let cal = calendar();
        // Thursday 2026-12-31, 10:00 EST: the last day listed, a trading day.
        assert_eq!(cal.session(utc(2026, 12, 31, 15, 0, 0)), Ok(()));
        // Friday 2027-01-01 is New Year's Day, which the lists end too early to name: the
        // calendar cannot say it is a holiday, so it says it does not know.
        let unlisted = cal.session(utc(2027, 1, 1, 15, 0, 0)).unwrap_err();
        assert!(matches!(unlisted, Closed::Unlisted(_)), "{unlisted}");
        assert!(unlisted.to_string().contains(THROUGH), "{unlisted}");
        // A weekend past the end is no different: the operator needs to hear it either way.
        assert!(matches!(
            cal.session(utc(2027, 1, 2, 15, 0, 0)),
            Err(Closed::Unlisted(_))
        ));
    }

    #[test]
    fn an_early_close_day_closes_at_one_pm() {
        let cal = calendar();
        // The day after Thanksgiving: 12:30 EST is open, 12:59:00 is inside the margin.
        assert_eq!(cal.session(utc(2026, 11, 27, 17, 30, 0)), Ok(()));
        assert!(cal.session(utc(2026, 11, 27, 17, 59, 0)).is_err());
        assert!(
            cal.session(utc(2026, 11, 27, 18, 30, 0)).is_err(),
            "13:30 EST"
        );
    }

    #[test]
    fn the_new_york_date_decides_not_the_utc_one() {
        // Friday 2026-10-09 23:30 EDT is already Saturday in UTC; Monday 2026-10-12 00:30 UTC
        // is still Sunday evening in New York. Both are closed, for the right reason.
        let cal = calendar();
        let friday_night = cal.session(utc(2026, 10, 10, 3, 30, 0)).unwrap_err();
        assert!(
            !friday_night.to_string().contains("weekend"),
            "{friday_night}"
        );
        let sunday_night = cal.session(utc(2026, 10, 12, 0, 30, 0)).unwrap_err();
        assert!(
            sunday_night.to_string().contains("weekend"),
            "{sunday_night}"
        );
    }

    #[test]
    fn the_calendar_refuses_what_can_only_be_a_typo() {
        let weekend = Calendar::new(&["2026-10-10".to_owned()], &[], THROUGH, 60, 60).unwrap_err();
        assert!(weekend.contains("2026-10-10"), "{weekend}");
        let both = Calendar::new(
            &["2026-11-26".to_owned()],
            &["2026-11-26".to_owned()],
            THROUGH,
            60,
            60,
        )
        .unwrap_err();
        assert!(both.contains("both"), "{both}");
        assert!(Calendar::new(&["2026-11-31".to_owned()], &[], THROUGH, 60, 60).is_err());
        assert!(Calendar::new(&[], &[], "2026-12-32", 60, 60).is_err());
        // An early close leaves 3.5h; margins that eat it leave no session at all.
        assert!(Calendar::new(&[], &[], THROUGH, 7_200, 5_400).is_err());
        assert!(Calendar::new(&[], &[], THROUGH, 7_200, 5_399).is_ok());
    }

    #[test]
    fn the_days_left_count_new_york_dates_to_the_last_listed_one() {
        let cal = calendar();
        assert_eq!(cal.days_left(utc(2026, 12, 31, 15, 0, 0)), 0);
        // 2026-12-31 03:00 UTC is still the 30th in New York.
        assert_eq!(cal.days_left(utc(2026, 12, 31, 3, 0, 0)), 1);
        assert_eq!(cal.days_left(utc(2026, 12, 1, 15, 0, 0)), 30);
        assert_eq!(cal.days_left(utc(2027, 1, 2, 15, 0, 0)), -2);
    }

    #[test]
    fn a_listed_day_past_the_calendars_end_is_refused() {
        // Listing 2027's Independence Day while saying the lists end in 2026 is a calendar
        // that contradicts itself.
        let err = Calendar::new(&["2027-07-05".to_owned()], &[], THROUGH, 60, 60).unwrap_err();
        assert!(err.contains("2027-07-05") && err.contains(THROUGH), "{err}");
        assert!(Calendar::new(&[], &["2027-11-26".to_owned()], THROUGH, 60, 60).is_err());
    }

    #[test]
    fn margins_too_large_to_add_up_are_refused_not_wrapped() {
        // Wrapped, u64::MAX + 1 is 0: no margin at all, and quoting from 09:30:00.
        assert!(Calendar::new(&[], &[], THROUGH, u64::MAX, 1).is_err());
        assert!(Calendar::new(&[], &[], THROUGH, 1, u64::MAX).is_err());
    }
}
