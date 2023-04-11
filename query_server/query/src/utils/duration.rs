use std::time::Duration;

use models::duration::{DAY, HOUR, MINUTE, MS, SECOND};

pub fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    if s.is_empty() {
        return Err("Empty string".to_string());
    }
    let size_len = s
        .to_string()
        .chars()
        .take_while(|c| char::is_ascii_digit(c) || ['.'].contains(c))
        .count();
    let (digits, unit) = s.split_at(size_len);

    let digits = digits.parse::<f64>().map_err(|err| err.to_string())?;

    let unit = match unit {
        "d" => DAY,
        "h" => HOUR,
        "m" => MINUTE,
        "s" => SECOND,
        "ms" => MS,
        _ => return Err("Only support d, h, m, s, ms".to_string()),
    };

    let millis = digits * (unit as f64);

    if millis.floor() != millis || millis < 1.0 {
        return Err(format!(
            "Must be greater than or equal to 1ms, but found: {s}"
        ));
    }

    Ok(Duration::from_millis(millis as u64))
}
