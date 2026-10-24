//! Parsing and validation of message templates, usable in const context.

use core::fmt;

use crate::Property;

/// FNV-1a (32 bit) of the template text: the stable [`EventId`](crate::EventId).
pub const fn event_id(text: &str) -> u32 {
    let b = text.as_bytes();
    let mut h: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < b.len() {
        h ^= b[i] as u32;
        h = h.wrapping_mul(0x0100_0193);
        i += 1;
    }
    h
}

const fn is_upper(c: u8) -> bool {
    c.is_ascii_uppercase()
}

const fn is_alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}

/// Checks a template against property names:
/// - holes are `{Name}`, `{@Name}`, `{$Name}` or `{Name:format}`; `{{` and `}}` are literal braces;
/// - names are PascalCase identifiers (`[A-Z][A-Za-z0-9]*`);
/// - every hole has a property and every property a hole, in the same order, without repeats.
pub const fn validate(text: &str, names: &[&str]) -> bool {
    let b = text.as_bytes();
    let mut i = 0;
    let mut hole = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'}' {
            if i + 1 < b.len() && b[i + 1] == b'}' {
                i += 2;
                continue;
            }
            return false;
        }
        if c != b'{' {
            i += 1;
            continue;
        }
        if i + 1 < b.len() && b[i + 1] == b'{' {
            i += 2;
            continue;
        }
        i += 1;
        if i < b.len() && (b[i] == b'@' || b[i] == b'$') {
            i += 1;
        }
        let start = i;
        if i >= b.len() || !is_upper(b[i]) {
            return false;
        }
        while i < b.len() && is_alnum(b[i]) {
            i += 1;
        }
        let end = i;
        if i < b.len() && b[i] == b':' {
            while i < b.len() && b[i] != b'}' {
                i += 1;
            }
        }
        if i >= b.len() || b[i] != b'}' {
            return false;
        }
        i += 1;
        if hole >= names.len() || !same(b, start, end, names[hole].as_bytes()) {
            return false;
        }
        hole += 1;
    }
    hole == names.len()
}

const fn same(b: &[u8], start: usize, end: usize, name: &[u8]) -> bool {
    if end - start != name.len() {
        return false;
    }
    let mut k = 0;
    while k < name.len() {
        if b[start + k] != name[k] {
            return false;
        }
        k += 1;
    }
    true
}

/// Writes the template with property values in place of the holes.
pub(crate) fn render(text: &str, props: &[Property<'_>], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut rest = text;
    let mut hole = 0;
    while let Some(pos) = rest.find(['{', '}']) {
        let (lit, tail) = rest.split_at(pos);
        f.write_str(lit)?;
        if tail.starts_with("{{") || tail.starts_with("}}") {
            f.write_str(&tail[..1])?;
            rest = &tail[2..];
            continue;
        }
        let close = tail.find('}').unwrap_or(tail.len() - 1);
        match props.get(hole) {
            Some(p) => write!(f, "{}", p.value)?,
            None => f.write_str(&tail[..=close])?,
        }
        hole += 1;
        rest = &tail[close + 1..];
    }
    f.write_str(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l1_validation_rules() {
        assert!(validate("Plain text", &[]));
        assert!(validate(
            "Peer {PeerId} from {RemoteEndpoint}",
            &["PeerId", "RemoteEndpoint"]
        ));
        assert!(validate(
            "Stats {@Stats} {$Reason} {RttMs:0.0}",
            &["Stats", "Reason", "RttMs"]
        ));
        assert!(validate("Literal {{braces}} kept", &[]));
        assert!(!validate("Peer {PeerId}", &[]), "hole without property");
        assert!(!validate("Peer", &["PeerId"]), "property without hole");
        assert!(!validate("Peer {peerId}", &["peerId"]), "not PascalCase");
        assert!(!validate("Peer {Peer_Id}", &["Peer_Id"]), "underscore");
        assert!(!validate("{A} {B}", &["B", "A"]), "order");
        assert!(!validate("Unclosed {PeerId", &["PeerId"]));
        assert!(!validate("Stray } brace", &[]));
        assert!(!validate("{}", &[]));
    }
}
