//! One module per venue: where to connect, what to send to be told about a market, what
//! its frames look like, and how it spells a market. Nothing else; see `venue.rs`.

pub mod binance;
pub mod bitget;
pub mod bybit;
pub mod coinbase;
pub mod coingecko;
pub mod gate;
pub mod kraken;
pub mod kucoin;
pub mod mexc;
pub mod okx;

/// An exchange's millisecond timestamp as a wall-clock time: what a venue that stamps its
/// frames says the price is as of, for the feed's source age.
pub fn epoch_ms(ms: u64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms)
}

/// [`epoch_ms`] read off a frame, where a venue sends its millisecond stamp as a number
/// (KuCoin's `timestamp`, Bitget's top-level `ts`) or as a string of digits (Bybit's `ts`,
/// Bitget's per-ticker `ts`). Anything else reads as no stamp rather than refusing the
/// price: the price is what the frame is for.
pub fn epoch_ms_value(value: &serde_json::Value) -> Option<std::time::SystemTime> {
    match value {
        serde_json::Value::Number(n) => n.as_u64().map(epoch_ms),
        serde_json::Value::String(s) => s.parse::<u64>().ok().map(epoch_ms),
        _ => None,
    }
}

/// An RFC 3339 stamp in UTC, `2023-09-25T09:04:31.742648Z`, as Kraken and Coinbase send
/// one, as a wall-clock time. Only that shape: a date, `T`, a time with any number of
/// fraction digits, `Z`. Anything else reads as no stamp. Written out rather than taken
/// from a date crate because this is the one place a calendar date is read, and the
/// arithmetic is a dozen lines.
pub fn rfc3339_utc(text: &str) -> Option<std::time::SystemTime> {
    let text = text.strip_suffix('Z')?;
    let (date, time) = text.split_once('T')?;
    let mut date = date.split('-');
    let (year, month, day): (i64, u32, u32) = (
        date.next()?.parse().ok()?,
        date.next()?.parse().ok()?,
        date.next()?.parse().ok()?,
    );
    if date.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut clock = clock.split(':');
    let (hour, minute, second): (u64, u64, u64) = (
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
        clock.next()?.parse().ok()?,
    );
    if clock.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    if !fraction.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Digits past the ninth are below a nanosecond.
    let nanos: u64 = fraction
        .chars()
        .take(9)
        .map(|c| u64::from(c as u8 - b'0'))
        .fold(0, |n, d| n * 10 + d)
        * 10u64.pow(9u32.saturating_sub(fraction.len().min(9) as u32));
    // Days since 1970-01-01, by the proleptic Gregorian calendar (Howard Hinnant's
    // days_from_civil): March-based years so the leap day falls at the end.
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * i64::from(m) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = u64::try_from(days).ok()? * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(std::time::UNIX_EPOCH + std::time::Duration::new(secs, nanos as u32))
}

/// The quotes a market may be in for a pair quoted in `quote`, besides `quote` itself:
/// for a USDC or USDT pair, the other of the two and USD, since the busiest market an
/// exchange has for a dollar pair is often in one of the others (Coinbase's ETH-USDC is
/// delisted; ETH-USD is where it trades). Nothing for any other quote. The lookup picks a
/// market by this, and the startup cross-check accepts one by it, so the two agree.
pub fn dollar_stand_ins(quote: &str) -> &'static [&'static str] {
    match quote.to_uppercase().as_str() {
        "USDC" => &["USDT", "USD"],
        "USDT" => &["USDC", "USD"],
        _ => &[],
    }
}

/// The spelling most venues share for the underlying of a wrapped ERC-20: they trade
/// ETH and BTC, not WETH, WBTC or cbBTC. Every wrapper `discover::underlying` looks up by
/// its underlying's markets has to be here too, or the lookup finds those markets and then
/// discards them all for naming a different base (a test holds the two lists together).
pub fn unwrap_alias(symbol: &str) -> &str {
    match symbol {
        "WETH" => "ETH",
        "WBTC" | "CBBTC" => "BTC",
        other => other,
    }
}

#[cfg(test)]
mod stamp_tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn a_millisecond_stamp_reads_as_a_number_or_a_string_of_digits() {
        let at = epoch_ms(1_700_000_000_000);
        assert_eq!(
            epoch_ms_value(&serde_json::json!(1_700_000_000_000u64)),
            Some(at)
        );
        assert_eq!(
            epoch_ms_value(&serde_json::json!("1700000000000")),
            Some(at)
        );
        assert_eq!(epoch_ms_value(&serde_json::json!("soon")), None);
        assert_eq!(epoch_ms_value(&serde_json::json!(null)), None);
        assert_eq!(epoch_ms_value(&serde_json::json!(-1)), None);
    }

    /// Checked against known epoch seconds: Kraken's and Coinbase's own example stamps,
    /// the epoch, a leap day, and the last second of a year.
    #[test]
    fn an_rfc3339_utc_stamp_reads_to_the_nanosecond() {
        let at = |secs: u64, nanos: u32| UNIX_EPOCH + Duration::new(secs, nanos);
        assert_eq!(
            rfc3339_utc("2023-09-25T09:04:31.742648Z"),
            Some(at(1_695_632_671, 742_648_000))
        );
        assert_eq!(
            rfc3339_utc("2022-10-19T23:28:22.061769Z"),
            Some(at(1_666_222_102, 61_769_000))
        );
        assert_eq!(rfc3339_utc("1970-01-01T00:00:00Z"), Some(at(0, 0)));
        assert_eq!(
            rfc3339_utc("2024-02-29T12:00:00Z"),
            Some(at(1_709_208_000, 0))
        );
        assert_eq!(
            rfc3339_utc("2025-12-31T23:59:59.5Z"),
            Some(at(1_767_225_599, 500_000_000))
        );
        // Past nine fraction digits is below a nanosecond and ignored.
        assert_eq!(
            rfc3339_utc("1970-01-01T00:00:00.1234567891Z"),
            Some(at(0, 123_456_789))
        );
        for bad in [
            "2023-09-25T09:04:31.742648",
            "2023-09-25 09:04:31Z",
            "2023-09-25T09:04:31+02:00",
            "2023-13-25T09:04:31Z",
            "2023-09-25T24:04:31Z",
            "2023-09-25T09:04:31.12a4Z",
            "not a date",
            "",
        ] {
            assert_eq!(rfc3339_utc(bad), None, "{bad}");
        }
    }
}
