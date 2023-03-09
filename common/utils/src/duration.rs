use std::error::Error;
use std::time::Duration;

const UNITS_LIST: [&[char]; 7] = [
    &['n', 's'],
    &['u', 's'],
    &['μ', 's'],
    &['m', 's'],
    &['s'],
    &['m'],
    &['h'],
];
const SCALES_LIST: [u64; 7] = [
    1,
    1_000,
    1_000_000,
    1_000_000,
    1_000_000_000,
    60_000_000_000,
    60 * 60_000_000_000,
];

/// Parse ([0-9]+[a-z]+) to Duration.
pub fn parse_duration(duration_str: &str) -> Result<Duration, Box<dyn Error>> {
    if duration_str == "0" {
        return Ok(Duration::from_nanos(0));
    }
    if duration_str.is_empty() {
        return Err(From::from(format!("Invalid duration '{}'", duration_str)));
    }

    let chars: Vec<char> = duration_str.chars().collect();
    let mut s = chars.as_slice();
    let mut v;

    // Consume integers
    (v, s) = match consume_int(s) {
        Ok((val, sli)) => (val, sli),
        Err(e) => {
            return Err(From::from(format!(
                "Invalid duration ({}): '{}'",
                e, duration_str
            )))
        }
    };

    // Consume unit.
    let mut i = 0_usize;
    for c in s {
        if *c == '.' || '0' <= *c && *c <= '9' {
            break;
        }
        i += 1;
    }
    if i == 0 {
        return Err(From::from(format!(
            "Missing unit in duration '{}'",
            duration_str
        )));
    }
    let mut unit = 0;
    let u = &s[..i];
    for (ui, cu) in UNITS_LIST.into_iter().enumerate() {
        if cu == u {
            unit = SCALES_LIST[ui];
        }
    }
    if unit == 0 {
        return Err(From::from(format!(
            "Unknown unit '{:?}' in duration '{}'",
            u, duration_str
        )));
    }

    if v > (1 << 63) / unit {
        return Err(From::from(format!(
            "Invalid duration (overflow) '{}'",
            duration_str
        )));
    }
    v *= unit;
    if v > (1 << 63) - 1 {
        return Err(From::from(format!(
            "Invalid duration (overflow) '{}'",
            duration_str
        )));
    }

    Ok(Duration::from_nanos(v))
}

fn consume_int(s: &[char]) -> Result<(u64, &[char]), Box<dyn Error>> {
    let mut i = 0_usize;
    let mut x = 0_u64;
    for c in s {
        if *c < '0' || *c > '9' {
            break;
        }
        if x > (1 << 63) / 10 {
            return Err(From::from("overflow"));
        }
        x = x * 10 + *c as u64 - '0' as u64;
        if x > 1 << 63 {
            return Err(From::from("overflow"));
        }
        i += 1;
    }
    Ok((x, &s[i..]))
}
