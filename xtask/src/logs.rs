//! Log event catalog (`docs/LOG_EVENTS.md`) and the CLEF log viewer.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use netlib_log::{event_id, validate};

/// One `netlog!` call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub id: u32,
    pub level: String,
    pub template: String,
    pub source: String,
}

/// Events emitted by netlib-log itself rather than through `netlog!`.
const BUILTIN: &[(&str, &str, &str)] = &[(
    "Warn",
    "Suppressed {Count} events {SuppressedEventId}",
    "crates/netlib-log/src/limit.rs",
)];

/// Parses a Rust string literal starting at `s[0] == '"'`; returns its value.
fn string_literal(s: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = s.chars().skip(1);
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                '\\' => out.push('\\'),
                '"' => out.push('"'),
                '\n' => {
                    // Line continuation: skip leading whitespace of the next line.
                    let rest: String = chars.by_ref().skip_while(|c| c.is_whitespace()).collect();
                    let mut v = string_literal(&format!("\"{rest}"))?;
                    out.push_str(&std::mem::take(&mut v));
                    return Some(out);
                }
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
    None
}

/// Finds every `netlog!` call in the `.rs` files below `dir`.
pub fn scan(root: &Path, dir: &str) -> Vec<CatalogEntry> {
    let mut out = Vec::new();
    let mut files = Vec::new();
    collect_files(&root.join(dir), &mut files);
    files.sort();
    for f in files {
        let Ok(text) = std::fs::read_to_string(&f) else {
            continue;
        };
        let rel = f.strip_prefix(root).unwrap_or(&f).to_string_lossy().replace('\\', "/");
        let mut rest = text.as_str();
        while let Some(pos) = rest.find("netlog!(") {
            let call = &rest[pos + "netlog!(".len()..];
            rest = call;
            let Some(q) = call.find('"') else { break };
            let args: Vec<&str> = call[..q].split(',').map(str::trim).collect();
            // Skip the macro definition and docs (`$level:ident`, `Level`).
            let Some(level) = args
                .get(1)
                .filter(|l| ["Trace", "Debug", "Info", "Warn", "Error"].contains(l))
            else {
                continue;
            };
            let Some(template) = string_literal(&call[q..]) else {
                continue;
            };
            out.push(CatalogEntry {
                id: event_id(&template),
                level: level.to_string(),
                template,
                source: rel.clone(),
            });
        }
    }
    out
}

fn collect_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            collect_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// All events of the libraries, deduplicated by template.
pub fn catalog(root: &Path) -> Vec<CatalogEntry> {
    let mut all = Vec::new();
    for dir in ["crates/netlib-core/src", "crates/netlib/src", "crates/netlib-ffi/src"] {
        all.extend(scan(root, dir));
    }
    for (level, template, source) in BUILTIN {
        all.push(CatalogEntry {
            id: event_id(template),
            level: level.to_string(),
            template: template.to_string(),
            source: source.to_string(),
        });
    }
    let mut seen = BTreeMap::new();
    for e in all {
        seen.entry(e.template.clone()).or_insert(e);
    }
    let mut v: Vec<CatalogEntry> = seen.into_values().collect();
    let rank = |l: &str| ["Error", "Warn", "Info", "Debug", "Trace"].iter().position(|x| *x == l);
    v.sort_by(|a, b| rank(&a.level).cmp(&rank(&b.level)).then(a.template.cmp(&b.template)));
    v
}

/// Names of the holes of a template, in order.
pub fn holes(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = template;
    while let Some(i) = rest.find('{') {
        if rest[i..].starts_with("{{") {
            rest = &rest[i + 2..];
            continue;
        }
        let Some(j) = rest[i..].find('}') else { break };
        let hole = &rest[i + 1..i + j];
        let name = hole.trim_start_matches(['@', '$']).split(':').next().unwrap_or("");
        out.push(name.to_string());
        rest = &rest[i + j + 1..];
    }
    out
}

/// Checks L1/L2 rules over the catalog; returns the problems found.
pub fn lint(entries: &[CatalogEntry]) -> Vec<String> {
    let mut problems = Vec::new();
    let mut ids: BTreeMap<u32, &str> = BTreeMap::new();
    for e in entries {
        let names = holes(&e.template);
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        if !validate(&e.template, &refs) {
            problems.push(format!("invalid template {:?} in {}", e.template, e.source));
        }
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        if sorted.len() != names.len() {
            problems.push(format!("repeated hole in {:?}", e.template));
        }
        if let Some(other) = ids.insert(e.id, &e.template) {
            problems.push(format!(
                "EventId collision {:08x}: {other:?} and {:?}",
                e.id, e.template
            ));
        }
    }
    problems
}

pub fn catalog_markdown(entries: &[CatalogEntry]) -> String {
    let mut s = String::new();
    s.push_str(
        "# Log events\n\n\
         Generated by `cargo xtask log-catalog` from the `netlog!` calls; a test fails when it is out of date.\n\
         `EventId` is FNV-1a (32 bit) of the template: it changes only when the template text changes.\n\n\
         Properties used across events: `PeerId` (peer id as u64), `RemoteEndpoint`, `ConnId` (connection id), \
         `Seq` (packet sequence number), `Channel`.\n\n\
         | EventId | Level | Template | Source |\n|---------|-------|----------|--------|\n",
    );
    for e in entries {
        let _ = writeln!(
            s,
            "| `{:08x}` | {} | `{}` | `{}` |",
            e.id,
            e.level,
            e.template.replace('|', "\\|"),
            e.source
        );
    }
    s
}

/// Substitutes CLEF property values into `@mt`.
pub fn render_clef(ev: &serde_json::Map<String, serde_json::Value>) -> String {
    let mt = ev.get("@mt").and_then(|v| v.as_str()).unwrap_or("");
    let mut out = String::new();
    let mut rest = mt;
    while let Some(i) = rest.find(['{', '}']) {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        if tail.starts_with("{{") || tail.starts_with("}}") {
            out.push_str(&tail[..1]);
            rest = &tail[2..];
            continue;
        }
        let Some(j) = tail.find('}') else {
            out.push_str(tail);
            return out;
        };
        let name = tail[1..j]
            .trim_start_matches(['@', '$'])
            .split(':')
            .next()
            .unwrap_or("");
        match ev.get(name) {
            Some(serde_json::Value::String(s)) => out.push_str(s),
            Some(v) => out.push_str(&v.to_string()),
            None => out.push_str(&tail[..=j]),
        }
        rest = &tail[j + 1..];
    }
    out.push_str(rest);
    out
}

/// `cargo xtask logview FILE [--peer ID] [--event ID] [--min-level L] [--summary]`
pub fn logview(args: &[String]) -> Result<String, String> {
    let mut file = None;
    let (mut peer, mut event, mut min, mut summary) = (None, None, None, false);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--peer" => peer = it.next().cloned(),
            "--event" => event = it.next().map(|s| s.trim_start_matches("0x").to_ascii_lowercase()),
            "--min-level" => min = it.next().cloned(),
            "--summary" => summary = true,
            f => file = Some(f.to_string()),
        }
    }
    let file = file.ok_or("usage: cargo xtask logview FILE [--peer ID] [--event ID] [--min-level L] [--summary]")?;
    let text = std::fs::read_to_string(&file).map_err(|e| format!("{file}: {e}"))?;
    let levels = ["Verbose", "Debug", "Information", "Warning", "Error"];
    let min_rank = min
        .as_deref()
        .and_then(|m| levels.iter().position(|l| l.eq_ignore_ascii_case(m)))
        .unwrap_or(0);
    let mut out = String::new();
    let mut counts: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for (n, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).map_err(|e| format!("line {}: {e}", n + 1))?;
        let ev = v.as_object().ok_or(format!("line {}: not an object", n + 1))?;
        let level = ev.get("@l").and_then(|l| l.as_str()).unwrap_or("Information");
        let id = ev.get("@i").and_then(|i| i.as_str()).unwrap_or("");
        if levels.iter().position(|l| *l == level).unwrap_or(2) < min_rank {
            continue;
        }
        if event.as_deref().is_some_and(|e| e != id) {
            continue;
        }
        if let Some(p) = &peer {
            if ev.get("PeerId").map(|v| v.to_string()).as_deref() != Some(p.as_str()) {
                continue;
            }
        }
        let mt = ev.get("@mt").and_then(|m| m.as_str()).unwrap_or("").to_string();
        *counts.entry((id.to_string(), mt)).or_default() += 1;
        if let Some(r) = ev.get("Reason").and_then(|r| r.as_str()) {
            *reasons.entry(r.to_string()).or_default() += 1;
        }
        if !summary {
            let t = ev.get("@t").and_then(|t| t.as_str()).unwrap_or("");
            let short = match level {
                "Verbose" => "TRC",
                "Debug" => "DBG",
                "Warning" => "WRN",
                "Error" => "ERR",
                _ => "INF",
            };
            let _ = writeln!(out, "{t} {short} [{id}] {}", render_clef(ev));
        }
    }
    if summary {
        let _ = writeln!(out, "{:>7}  {:8}  template", "count", "event");
        let mut by_count: Vec<_> = counts.into_iter().collect();
        by_count.sort_by_key(|e| std::cmp::Reverse(e.1));
        for ((id, mt), c) in by_count {
            let _ = writeln!(out, "{c:>7}  {id:8}  {mt}");
        }
        if !reasons.is_empty() {
            let _ = writeln!(out, "\nreasons:");
            for (r, c) in reasons {
                let _ = writeln!(out, "{c:>7}  {r}");
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf()
    }

    #[test]
    fn l1_l2_catalog_is_valid_and_up_to_date() {
        let entries = catalog(&root());
        assert!(entries.len() >= 30, "found {} events", entries.len());
        assert_eq!(lint(&entries), Vec::<String>::new());
        let doc = std::fs::read_to_string(root().join("docs/LOG_EVENTS.md")).unwrap_or_default();
        assert!(
            doc == catalog_markdown(&entries),
            "docs/LOG_EVENTS.md is out of date: run `cargo xtask log-catalog`"
        );
    }

    #[test]
    fn string_literals() {
        assert_eq!(string_literal(r#""a \"b\" {C}" rest"#).as_deref(), Some(r#"a "b" {C}"#));
        assert_eq!(holes("x {A} {{y}} {@B} {$C:0.0}"), ["A", "B", "C"]);
    }

    #[test]
    fn l5_l7_clef_from_a_real_scenario_parses_renders_and_views() {
        use netlib_core::log::{Level, write_clef};
        use netlib_core::{ChannelConfig, Config, DeliveryMode, Event};
        use netlib_sim::{Link, LogCapture, Sim};
        use std::time::Duration;

        // A lossy session with an address change, captured as CLEF.
        #[derive(Clone)]
        struct Clef(std::sync::Arc<std::sync::Mutex<String>>, LogCapture);
        impl netlib_core::log::LogSink for Clef {
            fn enabled(&self, _: Level) -> bool {
                true
            }
            fn log(&mut self, r: &netlib_core::log::Record<'_>) {
                let mut s = self.0.lock().unwrap();
                write_clef(&mut *s, 1_700_000_000_000_000 + r.time_us, r).unwrap();
                s.push('\n');
                self.1.log(r);
            }
        }
        let clef = Clef(Default::default(), LogCapture::new(Level::Trace));
        let config = |server| Config {
            protocol_id: 1,
            accept_incoming: server,
            channels: vec![ChannelConfig::new(DeliveryMode::ReliableOrdered)],
            stats_interval: Some(Duration::from_millis(500)),
            ..Config::default()
        };
        let mut sim = Sim::new(
            9,
            Link {
                loss: 0.2,
                ..Link::default()
            },
        );
        sim.add_node(config(true)).unwrap();
        sim.add_node(config(false)).unwrap();
        sim.manager(1).set_log_sink(Some(Box::new(clef.clone())));
        let (addr, now) = (sim.addr(0), sim.now);
        let peer = sim.manager(1).connect(now, addr, &[]).unwrap();
        sim.transmit();
        for t in 0..64 * 4 {
            if t == 64 * 2 {
                sim.set_addr(1, "10.1.2.3:4".parse().unwrap());
            }
            sim.tick(Duration::from_micros(15_625), |i, m, now| {
                while let Some(e) = m.poll_event() {
                    if let Event::ConnectRequest { peer, .. } = e {
                        m.accept(now, peer).unwrap();
                    }
                }
                if i == 1 {
                    let _ = m.send(peer, 0, &[t as u8; 20]);
                }
            });
        }
        let text = clef.0.lock().unwrap().clone();
        let captured = clef.1.entries();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), captured.len());
        assert!(lines.len() > 100);
        for (line, entry) in lines.iter().zip(&captured) {
            let v: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            let ev = v.as_object().unwrap();
            assert!(ev["@t"].as_str().unwrap().starts_with("2023-11-14T"));
            assert_eq!(ev["@mt"].as_str().unwrap(), entry.template);
            assert_eq!(ev["@i"].as_str().unwrap(), format!("{:08x}", entry.id));
            assert_eq!(
                render_clef(ev),
                entry.message,
                "rendering from CLEF matches the original"
            );
        }
        // The viewer reads it back.
        let path = std::env::temp_dir().join(format!("netlib-l5-{}.clef", std::process::id()));
        std::fs::write(&path, &text).unwrap();
        let summary = logview(&[path.to_string_lossy().to_string(), "--summary".into()]).unwrap();
        assert!(summary.contains("Retransmitted bytes"), "{summary}");
        let timeline = logview(&[
            path.to_string_lossy().to_string(),
            "--min-level".into(),
            "Information".into(),
        ])
        .unwrap();
        assert!(timeline.lines().all(|l| !l.contains(" DBG ") && !l.contains(" TRC ")));
        let _ = std::fs::remove_file(path);
    }
}
