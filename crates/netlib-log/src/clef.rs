//! Compact Log Event Format (https://clef-json.org/): one JSON object per line.

use core::fmt::{self, Write};

use crate::{Level, Record, Value};

/// Writes `secs` since the Unix epoch plus microseconds as ISO 8601 UTC.
pub fn write_iso8601(w: &mut impl Write, unix_secs: u64, micros: u32) -> fmt::Result {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    // Civil date from days (H. Hinnant).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    write!(
        w,
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{micros:06}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

fn json_str(w: &mut impl Write, s: &str) -> fmt::Result {
    w.write_char('"')?;
    for c in s.chars() {
        match c {
            '"' => w.write_str("\\\"")?,
            '\\' => w.write_str("\\\\")?,
            '\n' => w.write_str("\\n")?,
            '\r' => w.write_str("\\r")?,
            '\t' => w.write_str("\\t")?,
            c if (c as u32) < 0x20 => write!(w, "\\u{:04x}", c as u32)?,
            c => w.write_char(c)?,
        }
    }
    w.write_char('"')
}

/// A `Write` adapter that JSON-escapes everything written through it.
struct Escaped<'w, W: Write>(&'w mut W);

impl<W: Write> Write for Escaped<'_, W> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut buf = [0u8; 4];
        for c in s.chars() {
            match c {
                '"' | '\\' | '\n' | '\r' | '\t' => json_str_char(self.0, c)?,
                c if (c as u32) < 0x20 => write!(self.0, "\\u{:04x}", c as u32)?,
                c => self.0.write_str(c.encode_utf8(&mut buf))?,
            }
        }
        Ok(())
    }
}

fn json_str_char(w: &mut impl Write, c: char) -> fmt::Result {
    w.write_str(match c {
        '"' => "\\\"",
        '\\' => "\\\\",
        '\n' => "\\n",
        '\r' => "\\r",
        _ => "\\t",
    })
}

fn json_value(w: &mut impl Write, v: &Value<'_>) -> fmt::Result {
    match *v {
        Value::U64(x) => write!(w, "{x}"),
        Value::I64(x) => write!(w, "{x}"),
        Value::F64(x) if x.is_finite() => write!(w, "{x}"),
        Value::F64(_) => w.write_str("null"),
        Value::Bool(x) => write!(w, "{x}"),
        Value::Str(s) => json_str(w, s),
        Value::Addr(a) => {
            w.write_char('"')?;
            write!(Escaped(w), "{a}")?;
            w.write_char('"')
        }
        Value::Duration(d) => write!(w, "{}", crate::duration_ms(d)),
    }
}

/// Writes one CLEF line (without the newline): `@t` from `unix_micros`, `@mt`, `@l` (omitted
/// for Information, as in the format), `@i` and the properties.
pub fn write_clef(w: &mut impl Write, unix_micros: u64, record: &Record<'_>) -> fmt::Result {
    w.write_str("{\"@t\":\"")?;
    write_iso8601(w, unix_micros / 1_000_000, (unix_micros % 1_000_000) as u32)?;
    w.write_str("\",\"@mt\":")?;
    json_str(w, record.template.text)?;
    if record.template.level != Level::Info {
        write!(w, ",\"@l\":\"{}\"", record.template.level.clef_name())?;
    }
    write!(w, ",\"@i\":\"{:08x}\"", record.template.id)?;
    for p in record.properties {
        w.write_str(",")?;
        json_str(w, p.name)?;
        w.write_char(':')?;
        json_value(w, &p.value)?;
    }
    w.write_char('}')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Property, Template, event_id};
    use alloc::format;
    use alloc::string::String;

    #[test]
    fn iso8601() {
        let mut s = String::new();
        write_iso8601(&mut s, 0, 0).unwrap();
        assert_eq!(s, "1970-01-01T00:00:00.000000Z");
        s.clear();
        // 2026-10-24T10:40:21Z
        write_iso8601(&mut s, 1_792_838_421, 123_456).unwrap();
        assert_eq!(s, "2026-10-24T10:40:21.123456Z");
        s.clear();
        write_iso8601(&mut s, 951_782_400, 0).unwrap(); // leap day
        assert_eq!(s, "2000-02-29T00:00:00.000000Z");
    }

    #[test]
    fn l5_clef_line() {
        static T: Template = Template {
            text: "Peer {PeerId} said \"{Text}\"",
            level: Level::Warn,
            id: event_id("Peer {PeerId} said \"{Text}\""),
        };
        let props = [
            Property {
                name: "PeerId",
                value: Value::U64(3),
            },
            Property {
                name: "Text",
                value: Value::Str("a\"b\\c\n"),
            },
        ];
        let r = Record {
            template: &T,
            time_us: 0,
            properties: &props,
        };
        let mut s = String::new();
        write_clef(&mut s, 1_000_000, &r).unwrap();
        assert_eq!(
            s,
            format!(
                r#"{{"@t":"1970-01-01T00:00:01.000000Z","@mt":"Peer {{PeerId}} said \"{{Text}}\"","@l":"Warning","@i":"{:08x}","PeerId":3,"Text":"a\"b\\c\n"}}"#,
                T.id
            )
            .replace("c\n", "c\\n")
        );
    }
}
