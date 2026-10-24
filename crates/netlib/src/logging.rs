//! Ready-made log sinks: CLEF (JSON lines for Seq, Serilog, `jq`) and plain text.

use std::fmt::Write as _;
use std::io::Write;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use netlib_core::log::{Level, LogSink, Record, write_clef};

/// Maps the manager's monotonic microseconds to Unix time.
#[derive(Debug, Clone, Copy)]
pub struct WallClock {
    unix_micros_at_epoch: u64,
}

impl WallClock {
    /// For a manager created at `epoch` (see [`Manager::epoch`](crate::Manager::epoch)).
    pub fn new(epoch: Instant) -> Self {
        let now_unix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let since_epoch = epoch.elapsed();
        WallClock {
            unix_micros_at_epoch: (now_unix.saturating_sub(since_epoch)).as_micros() as u64,
        }
    }

    /// A clock where manager time 0 is the Unix epoch; for tests.
    pub fn fixed(unix_micros_at_epoch: u64) -> Self {
        WallClock { unix_micros_at_epoch }
    }

    /// Unix microseconds of a record.
    pub fn unix_micros(&self, record: &Record<'_>) -> u64 {
        self.unix_micros_at_epoch + record.time_us
    }
}

/// Writes one CLEF JSON object per line.
pub struct ClefSink<W> {
    out: W,
    min: Level,
    clock: WallClock,
    line: String,
}

impl<W: Write> ClefSink<W> {
    /// Writes events of `min` level and above to `out`.
    pub fn new(out: W, min: Level, clock: WallClock) -> Self {
        ClefSink {
            out,
            min,
            clock,
            line: String::with_capacity(256),
        }
    }

    /// The writer.
    pub fn into_inner(self) -> W {
        self.out
    }
}

impl<W: Write> LogSink for ClefSink<W> {
    fn enabled(&self, level: Level) -> bool {
        level >= self.min
    }

    fn log(&mut self, record: &Record<'_>) {
        self.line.clear();
        if write_clef(&mut self.line, self.clock.unix_micros(record), record).is_ok() {
            self.line.push('\n');
            let _ = self.out.write_all(self.line.as_bytes());
        }
    }
}

/// Writes `time level [EventId] rendered message` lines for humans.
pub struct TextSink<W> {
    out: W,
    min: Level,
    clock: WallClock,
    line: String,
}

impl<W: Write> TextSink<W> {
    /// Writes events of `min` level and above to `out`.
    pub fn new(out: W, min: Level, clock: WallClock) -> Self {
        TextSink {
            out,
            min,
            clock,
            line: String::with_capacity(256),
        }
    }
}

impl<W: Write> LogSink for TextSink<W> {
    fn enabled(&self, level: Level) -> bool {
        level >= self.min
    }

    fn log(&mut self, record: &Record<'_>) {
        self.line.clear();
        let us = self.clock.unix_micros(record);
        let _ = netlib_core::log::write_iso8601(&mut self.line, us / 1_000_000, (us % 1_000_000) as u32);
        let level = match record.template.level {
            Level::Trace => "TRC",
            Level::Debug => "DBG",
            Level::Info => "INF",
            Level::Warn => "WRN",
            Level::Error => "ERR",
        };
        let _ = writeln!(self.line, " {level} [{:08x}] {}", record.template.id, record.render());
        let _ = self.out.write_all(self.line.as_bytes());
    }
}
