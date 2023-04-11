use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
pub enum StreamTriggerInterval {
    Once,
    Interval(Duration),
}

impl FromStr for StreamTriggerInterval {
    type Err = String;

    /// Parse a string to StreamTriggerInterval
    ///
    /// # TimeUnit
    ///
    /// | Unit | Description |
    /// | ---- | ----------- |
    /// | ns    | Nanoseconds |
    /// | us    | Microseconds |
    /// | ms    | Milliseconds |
    /// | s    | Seconds     |
    /// | m    | Minutes     |
    /// | h    | Hours       |
    /// | d    | Days        |
    /// | w    | Weeks       |
    /// | mon  | Months      |
    /// | y    | Years       |
    ///
    /// # Examples
    ///
    /// ```
    /// use std::str::FromStr;
    /// use spi::query::config::StreamTriggerInterval;
    ///
    /// let interval = StreamTriggerInterval::from_str("once").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Once);
    ///
    /// let interval = StreamTriggerInterval::from_str("1s").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Interval(std::time::Duration::from_secs(1)));
    ///
    /// let interval = StreamTriggerInterval::from_str("1m").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Interval(std::time::Duration::from_secs(60)));
    ///
    /// let interval = StreamTriggerInterval::from_str("1h").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Interval(std::time::Duration::from_secs(3600)));
    ///
    /// let interval = StreamTriggerInterval::from_str("1s+500ms").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Interval(std::time::Duration::from_millis(1500)));
    ///
    /// let interval = StreamTriggerInterval::from_str("1m+30s").unwrap();
    /// assert_eq!(interval, StreamTriggerInterval::Interval(std::time::Duration::from_secs(90)));
    /// ```
    ///
    /// # Errors
    ///
    /// If the string is not a valid duration string, an error is returned.
    ///
    /// ```
    /// use std::str::FromStr;
    /// use spi::query::config::StreamTriggerInterval;
    ///
    /// let interval = StreamTriggerInterval::from_str("1.5");
    /// assert!(interval.is_err());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().trim() {
            "once" => Ok(StreamTriggerInterval::Once),
            _ => {
                let duration = duration_str::parse_std(s).map_err(|err| err.to_string())?;
                Ok(StreamTriggerInterval::Interval(duration))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use crate::query::config::StreamTriggerInterval;

    #[test]
    fn test() {
        let interval = StreamTriggerInterval::from_str("once").unwrap();
        assert_eq!(interval, StreamTriggerInterval::Once);

        let interval = StreamTriggerInterval::from_str("1s").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(1))
        );

        let interval = StreamTriggerInterval::from_str("1m").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(60))
        );

        let interval = StreamTriggerInterval::from_str("1h").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(3600))
        );

        let interval = StreamTriggerInterval::from_str("1d").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(24 * 3600))
        );

        let interval = StreamTriggerInterval::from_str("1w").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(7 * 24 * 3600))
        );

        let interval = StreamTriggerInterval::from_str("1mon").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(30 * 24 * 3600))
        );

        let interval = StreamTriggerInterval::from_str("1y").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(365 * 24 * 3600))
        );

        let interval = StreamTriggerInterval::from_str("1s+500ms").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_millis(1500))
        );

        let interval = StreamTriggerInterval::from_str("1m+30s").unwrap();
        assert_eq!(
            interval,
            StreamTriggerInterval::Interval(std::time::Duration::from_secs(90))
        );

        let interval = StreamTriggerInterval::from_str("1.5");
        assert!(interval.is_err());
    }
}
