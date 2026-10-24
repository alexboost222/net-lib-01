//! Structured logging with [message templates](https://messagetemplates.org/).
//!
//! An event is a constant template with named holes, a level, a stable [`EventId`] derived
//! from the template, and typed property values:
//!
//! ```
//! use netlib_log::{netlog, Level, LogSink, Record};
//!
//! struct Print;
//! impl LogSink for Print {
//!     fn enabled(&self, level: Level) -> bool { level >= Level::Info }
//!     fn log(&mut self, r: &Record<'_>) { println!("{}", r.render()); }
//! }
//!
//! let mut sink = Print;
//! netlog!(sink, Info, 0, "Peer {PeerId} connected from {RemoteEndpoint}", PeerId = 3u64, RemoteEndpoint = "10.0.0.1:9000");
//! ```
//!
//! The macro checks at compile time that the template is valid and that holes and
//! properties match one to one. When the level is disabled nothing is evaluated, formatted
//! or allocated. Rendering to text or [CLEF](https://clef-json.org/) happens in sinks.
#![no_std]

extern crate alloc;
#[cfg(test)]
extern crate std;

mod clef;
mod limit;
mod template;

pub use clef::{write_clef, write_iso8601};
pub use limit::RateLimited;
pub use template::{event_id, validate};

use core::fmt;
use core::net::SocketAddr;
use core::time::Duration;

/// Severity of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Per-datagram detail.
    Trace = 0,
    /// Diagnostics: drops, retransmissions, scheduler decisions.
    Debug = 1,
    /// Connection lifecycle and periodic statistics.
    Info = 2,
    /// Something went wrong but the connection survives or the cause is external.
    Warn = 3,
    /// A failure.
    Error = 4,
}

impl Level {
    /// The CLEF / Serilog name of the level.
    pub fn clef_name(self) -> &'static str {
        match self {
            Level::Trace => "Verbose",
            Level::Debug => "Debug",
            Level::Info => "Information",
            Level::Warn => "Warning",
            Level::Error => "Error",
        }
    }
}

/// Lowest level compiled in, controlled by the `strip-*` features.
pub const STATIC_MIN_LEVEL: Level = if cfg!(feature = "strip-debug") {
    Level::Info
} else if cfg!(feature = "strip-trace") {
    Level::Debug
} else {
    Level::Trace
};

/// Stable identifier of an event: FNV-1a (32 bit) of its template text.
pub type EventId = u32;

/// A typed property value.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    /// Unsigned integer.
    U64(u64),
    /// Signed integer.
    I64(i64),
    /// Floating point.
    F64(f64),
    /// Boolean.
    Bool(bool),
    /// Text.
    Str(&'a str),
    /// Socket address.
    Addr(SocketAddr),
    /// Duration, rendered in milliseconds with microsecond precision.
    Duration(Duration),
}

impl fmt::Display for Value<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Value::U64(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Str(v) => f.write_str(v),
            Value::Addr(v) => write!(f, "{v}"),
            Value::Duration(v) => write!(f, "{}", duration_ms(v)),
        }
    }
}

/// Milliseconds with microsecond precision, as durations are rendered.
pub fn duration_ms(d: Duration) -> f64 {
    d.as_micros() as f64 / 1000.0
}

/// Conversion into a [`Value`].
pub trait ToValue {
    /// The value.
    fn to_value(&self) -> Value<'_>;
}

macro_rules! to_value {
    ($variant:ident: $($t:ty),*) => {$(
        impl ToValue for $t {
            fn to_value(&self) -> Value<'_> {
                Value::$variant((*self).into())
            }
        }
    )*};
}
to_value!(U64: u8, u16, u32, u64);
to_value!(I64: i8, i16, i32, i64);
to_value!(F64: f32, f64);
to_value!(Bool: bool);
to_value!(Addr: SocketAddr);
to_value!(Duration: Duration);

impl ToValue for usize {
    fn to_value(&self) -> Value<'_> {
        Value::U64(*self as u64)
    }
}

impl ToValue for str {
    fn to_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}

impl ToValue for &str {
    fn to_value(&self) -> Value<'_> {
        Value::Str(self)
    }
}

impl ToValue for Value<'_> {
    fn to_value(&self) -> Value<'_> {
        *self
    }
}

/// A named property.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Property<'a> {
    /// PascalCase name, equal to a hole of the template.
    pub name: &'static str,
    /// The value.
    pub value: Value<'a>,
}

/// The constant part of an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Template {
    /// Template text, e.g. `"Peer {PeerId} connected"`.
    pub text: &'static str,
    /// Severity.
    pub level: Level,
    /// `event_id(text)`.
    pub id: EventId,
}

/// One log event.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    /// The template.
    pub template: &'static Template,
    /// Monotonic time of the event in microseconds, on the clock the core runs on.
    pub time_us: u64,
    /// Properties in template order.
    pub properties: &'a [Property<'a>],
}

impl<'a> Record<'a> {
    /// Value of a property.
    pub fn get(&self, name: &str) -> Option<Value<'a>> {
        self.properties.iter().find(|p| p.name == name).map(|p| p.value)
    }

    /// Renders the template with its values: `Peer 3 connected`.
    pub fn render(&self) -> Rendered<'_, 'a> {
        Rendered(self)
    }
}

/// Text rendering of a [`Record`]; see [`Record::render`].
pub struct Rendered<'r, 'a>(&'r Record<'a>);

impl fmt::Display for Rendered<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        template::render(self.0.template.text, self.0.properties, f)
    }
}

/// Receives log events. Implementations decide what is enabled, and format, filter or
/// forward events.
pub trait LogSink {
    /// Whether events of this level are wanted. Checked before any value is computed.
    fn enabled(&self, level: Level) -> bool;
    /// Handles one event.
    fn log(&mut self, record: &Record<'_>);
}

impl<S: LogSink + ?Sized> LogSink for &mut S {
    fn enabled(&self, level: Level) -> bool {
        (**self).enabled(level)
    }
    fn log(&mut self, record: &Record<'_>) {
        (**self).log(record)
    }
}

impl<S: LogSink + ?Sized> LogSink for alloc::boxed::Box<S> {
    fn enabled(&self, level: Level) -> bool {
        (**self).enabled(level)
    }
    fn log(&mut self, record: &Record<'_>) {
        (**self).log(record)
    }
}

/// A sink that drops everything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSink;

impl LogSink for NoSink {
    fn enabled(&self, _: Level) -> bool {
        false
    }
    fn log(&mut self, _: &Record<'_>) {}
}

/// Logs an event: `netlog!(sink, Level, time_us, "template", Name = value, ...)`.
///
/// `Level` is one of `Trace`, `Debug`, `Info`, `Warn`, `Error`. The template must be a
/// literal; holes and property names must match (checked at compile time).
#[macro_export]
macro_rules! netlog {
    ($sink:expr, $level:ident, $time:expr, $tpl:literal $(, $name:ident = $val:expr)* $(,)?) => {{
        const TEMPLATE: $crate::Template = $crate::Template {
            text: $tpl,
            level: $crate::Level::$level,
            id: $crate::event_id($tpl),
        };
        const _: () = ::core::assert!(
            $crate::validate($tpl, &[$(::core::stringify!($name)),*]),
            "invalid log template: holes and properties must match one to one, names in PascalCase"
        );
        // Method syntax so that both a sink and a `&mut` binding to one work.
        #[allow(unused_imports)]
        use $crate::LogSink as _;
        if $crate::Level::$level >= $crate::STATIC_MIN_LEVEL && $sink.enabled($crate::Level::$level) {
            // Borrowed temporaries live until the end of the block (lifetime extension).
            #[allow(non_snake_case)]
            let ($($name,)*) = ($(&$val,)*);
            let properties = [$($crate::Property {
                name: ::core::stringify!($name),
                value: $crate::ToValue::to_value($name),
            }),*];
            $sink.log(&$crate::Record {
                template: &TEMPLATE,
                time_us: $time,
                properties: &properties,
            });
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    #[derive(Default)]
    struct Capture {
        min: Option<Level>,
        lines: Vec<(Level, EventId, String)>,
    }

    impl LogSink for Capture {
        fn enabled(&self, level: Level) -> bool {
            self.min.is_some_and(|m| level >= m)
        }
        fn log(&mut self, r: &Record<'_>) {
            self.lines
                .push((r.template.level, r.template.id, r.render().to_string()));
        }
    }

    #[test]
    fn renders_and_filters() {
        let mut c = Capture {
            min: Some(Level::Debug),
            ..Capture::default()
        };
        let addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();
        netlog!(
            c,
            Info,
            1,
            "Peer {PeerId} connected from {RemoteEndpoint}",
            PeerId = 7u64,
            RemoteEndpoint = addr
        );
        netlog!(c, Trace, 2, "Datagram {Seq}", Seq = 1u16);
        netlog!(
            c,
            Warn,
            3,
            "Timeout after {Elapsed} ms",
            Elapsed = Duration::from_millis(1500)
        );
        assert_eq!(c.lines.len(), 2);
        assert_eq!(c.lines[0].2, "Peer 7 connected from 10.0.0.1:9000");
        assert_eq!(c.lines[0].1, event_id("Peer {PeerId} connected from {RemoteEndpoint}"));
        assert_eq!(c.lines[1].2, "Timeout after 1500 ms");
    }

    #[test]
    fn l3_disabled_level_evaluates_nothing() {
        let mut c = Capture::default();
        let mut evaluated = false;
        let mut side_effect = || {
            evaluated = true;
            1u64
        };
        netlog!(c, Error, 0, "Value {Value}", Value = side_effect());
        assert!(!evaluated, "arguments are evaluated only when enabled");
        assert!(c.lines.is_empty());
    }

    #[test]
    fn values_and_levels() {
        let mut c = Capture {
            min: Some(Level::Trace),
            ..Capture::default()
        };
        let s = alloc::string::String::from("owned");
        netlog!(
            c,
            Error,
            0,
            "{A} {B} {C} {D} {E} {F} {G} {H} {I} {J}",
            A = 1u8,
            B = -1i32,
            C = 2usize,
            D = 1.5f32,
            E = false,
            F = s.as_str(),
            G = *"slice",
            H = Value::U64(9),
            I = Duration::from_secs(1),
            J = 7u16
        );
        assert_eq!(c.lines[0].2, "1 -1 2 1.5 false owned slice 9 1000 7");
        let names: alloc::vec::Vec<_> = [Level::Trace, Level::Debug, Level::Info, Level::Warn, Level::Error]
            .iter()
            .map(|l| l.clef_name())
            .collect();
        assert_eq!(names, ["Verbose", "Debug", "Information", "Warning", "Error"]);
        let mut n = NoSink;
        netlog!(n, Error, 0, "never");
        let mut boxed: alloc::boxed::Box<dyn LogSink> = alloc::boxed::Box::new(Capture {
            min: Some(Level::Info),
            ..Capture::default()
        });
        assert!(boxed.enabled(Level::Info) && !boxed.enabled(Level::Debug));
        netlog!(boxed, Info, 0, "boxed {X}", X = 1u64);
        let r = Record {
            template: &Template {
                text: "{X}",
                level: Level::Info,
                id: 0,
            },
            time_us: 0,
            properties: &[],
        };
        assert_eq!(r.get("X"), None);
        assert_eq!(r.render().to_string(), "{X}");
    }

    #[test]
    fn l2_event_ids_are_stable() {
        // FNV-1a 32 of the template text: changing these values breaks log queries.
        assert_eq!(event_id(""), 0x811c_9dc5);
        assert_eq!(event_id("a"), 0xe40c_292c);
        assert_eq!(event_id("Peer {PeerId} connected"), event_id("Peer {PeerId} connected"));
        assert_ne!(
            event_id("Peer {PeerId} connected"),
            event_id("Peer {PeerId} disconnected")
        );
    }
}
