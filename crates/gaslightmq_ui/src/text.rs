//! How the UI writes times, lengths of time and the values of a call.

use gaslightmq_control_api::call::Call;
use gaslightmq_control_api::clock::{Duration, Instant};
use gaslightmq_control_api::field::{Field, FieldSource, FieldValue};

/// The time since the clock of the environment started, as a clock writes it: `00:02:11.432`.
///
/// ```
/// use gaslightmq_control_api::clock::{Duration, Instant};
/// use gaslightmq_ui::text::since_start;
///
/// let arrived = Instant::ZERO + Duration::from_millis(131_432);
/// assert_eq!(since_start(arrived), "00:02:11.432");
/// ```
pub fn since_start(instant: Instant) -> String {
    let millis = instant.duration_since(Instant::ZERO).as_millis();
    let (hours, minutes) = (millis / 3_600_000, millis / 60_000 % 60);
    let (seconds, rest) = (millis / 1000 % 60, millis % 1000);
    format!("{hours:02}:{minutes:02}:{seconds:02}.{rest:03}")
}

/// A length of time in the unit that reads best: `850 µs`, `14 ms`, `1.25 s`.
///
/// ```
/// use gaslightmq_control_api::clock::Duration;
/// use gaslightmq_ui::text::length;
///
/// assert_eq!(length(Duration::from_micros(850)), "850 µs");
/// assert_eq!(length(Duration::from_millis(14)), "14 ms");
/// assert_eq!(length(Duration::from_millis(1250)), "1.25 s");
/// ```
pub fn length(duration: Duration) -> String {
    let micros = duration.as_micros();
    if micros < 1000 {
        format!("{micros} µs")
    } else if micros < 1_000_000 {
        format!("{} ms", micros / 1000)
    } else {
        let seconds = format!("{:.2}", duration.as_secs_f64());
        let seconds = seconds.trim_end_matches('0').trim_end_matches('.');
        format!("{seconds} s")
    }
}

/// Every value the call has of `field`, in the order the call has them, each once.
pub fn values(call: &Call, field: Field) -> Vec<FieldValue> {
    let mut values: Vec<FieldValue> = Vec::new();
    for value in call.read(field).present().iter().flat_map(|present| present.iter()) {
        if !values.contains(value) {
            values.push(value.clone());
        }
    }
    values
}

/// The one value the call has of `field`, when it has one.
pub fn value(call: &Call, field: Field) -> Option<FieldValue> {
    values(call, field).into_iter().next()
}
