//! The log output of `gmq`: what the crates log, on the standard error.
//!
//! The standard output holds what a command did, one thing per line, so a script reads it without a log line in
//! between.

use tracing_subscriber::filter::LevelFilter;

use crate::cli::LogLevel;

/// Writes what the crates log at `level` and above to the standard error, from here on.
///
/// A process that has a log output already keeps it.
pub(crate) fn install(level: LogLevel) {
    let filter = match level {
        LogLevel::Off => LevelFilter::OFF,
        LogLevel::Error => LevelFilter::ERROR,
        LogLevel::Warn => LevelFilter::WARN,
        LogLevel::Info => LevelFilter::INFO,
        LogLevel::Debug => LevelFilter::DEBUG,
        LogLevel::Trace => LevelFilter::TRACE,
    };
    // `gmq` installs its log output once, before anything logs, so nothing else holds the place.
    let _ = tracing_subscriber::fmt()
        .with_max_level(filter)
        .with_writer(std::io::stderr)
        .try_init();
}
