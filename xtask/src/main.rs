//! Project automation: `cargo xtask <command>`.
//!
//! - `report [--milestone M1] [--all]` runs the checks and tests and prints the acceptance
//!   criteria table. Tests are linked to criteria by name: `r5_timeout_after_partition`
//!   covers criterion R5.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::process::{Command, ExitCode};

/// `(id, milestone, description)` for every criterion of docs/PLAN.md.
const CRITERIA: &[(&str, &str, &str)] = &[
    ("Q2", "M0", "Wire format documented (docs/WIRE_FORMAT.md)"),
    ("Q1", "M1", "fmt, clippy -D warnings, cargo deny"),
    ("R5", "M1", "Timeouts, keepalive, disconnect with reason"),
    ("R6", "M1", "Determinism: one seed, one outcome"),
    ("S1", "M1", "No amplification on unauthenticated packets"),
    ("S2", "M1", "No state before address validation (stateless cookie)"),
    ("R1", "M2", "All 5 delivery modes match their contract"),
    (
        "R2",
        "M2",
        "Correct under 0-20% loss, 0-300 ms latency, jitter, duplicates",
    ),
    ("R3", "M2", "u16 sequence wraparound (200k+ packets)"),
    ("R4", "M2", "RTO per RFC 6298, no retransmit storms"),
    ("B1", "M2", "Unreliable packet header <= 3 bytes"),
    ("B2", "M2", "Header with piggybacked ack <= 8 bytes"),
    ("B3", "M2", "Per-message overhead in a merged packet"),
    ("B4", "M2", "No standalone acks with bidirectional traffic"),
    ("PK1", "M3", "Datagram count close to the optimum"),
    ("PK2", "M3", "Datagrams except the last >= 95% full"),
    ("PK3", "M3", "Retransmits, new data and acks share packets"),
    ("PK4", "M3", "Fragments fill packets; reassembly under loss"),
    ("PK5", "M3", "send + flush allocation-free in steady state"),
    ("PR1", "M3", "High priority goes first under a budget"),
    ("PR2", "M3", "No starvation of low priority"),
    ("PR3", "M3", "TTL and replace-latest"),
    ("PR4", "M3", "Bandwidth budget within 5%"),
    ("PR5", "M3", "Scheduler cost"),
    ("P1", "M3", "Zero allocations per tick"),
];

const MILESTONES: &[&str] = &["M0", "M1", "M2", "M3", "M4", "M5", "M6", "M7", "M8", "M9", "M10"];

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("report") => report(&args[1..]),
        _ => {
            eprintln!("usage: cargo xtask report [--milestone M1] [--all]");
            ExitCode::FAILURE
        }
    }
}

fn run(cmd: &str, args: &[&str]) -> (bool, String) {
    eprintln!("$ {cmd} {}", args.join(" "));
    match Command::new(cmd).args(args).output() {
        Ok(o) => {
            let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
            s.push_str(&String::from_utf8_lossy(&o.stderr));
            (o.status.success(), s)
        }
        Err(e) => (false, e.to_string()),
    }
}

/// Criterion ids encoded in a test name: `pk1_pk2_foo` -> `["PK1", "PK2"]`.
fn criteria_of(test: &str) -> Vec<String> {
    let name = test.rsplit("::").next().unwrap_or(test);
    name.split('_')
        .map_while(|part| {
            let letters = part.trim_end_matches(|c: char| c.is_ascii_digit());
            let valid =
                !letters.is_empty() && letters.len() < part.len() && letters.chars().all(|c| c.is_ascii_lowercase());
            valid.then(|| part.to_ascii_uppercase())
        })
        .collect()
}

#[derive(Default)]
struct Result {
    passed: Vec<String>,
    failed: Vec<String>,
}

fn report(args: &[String]) -> ExitCode {
    let mut milestone = "M1".to_string();
    let mut all = false;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--milestone" => milestone = it.next().cloned().unwrap_or_default().to_uppercase(),
            "--all" => all = true,
            other => {
                eprintln!("unknown argument {other}");
                return ExitCode::FAILURE;
            }
        }
    }
    let upto = MILESTONES.iter().position(|m| *m == milestone).unwrap_or(1);
    let in_scope = |m: &str| all || MILESTONES.iter().position(|x| *x == m).is_some_and(|i| i <= upto);

    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let (fmt_ok, _) = run(&cargo, &["fmt", "--all", "--check"]);
    let (clippy_ok, clippy_out) = run(
        &cargo,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--quiet",
            "--",
            "-D",
            "warnings",
        ],
    );
    let (deny_ok, deny_out) = run(&cargo, &["deny", "check"]);
    let deny_missing = deny_out.contains("no such command");
    let (tests_ok, test_out) = run(&cargo, &["test", "--workspace", "--no-fail-fast"]);

    let mut results: BTreeMap<String, Result> = BTreeMap::new();
    let mut total = (0usize, 0usize);
    for line in test_out.lines() {
        let Some(rest) = line.strip_prefix("test ") else {
            continue;
        };
        let Some((name, status)) = rest.rsplit_once(" ... ") else {
            continue;
        };
        let ok = match status.trim() {
            "ok" => true,
            "FAILED" => false,
            _ => continue,
        };
        total.0 += usize::from(ok);
        total.1 += 1;
        for id in criteria_of(name) {
            let r = results.entry(id).or_default();
            if ok { &mut r.passed } else { &mut r.failed }.push(name.to_string());
        }
    }
    let q2_ok = std::fs::read_to_string("docs/WIRE_FORMAT.md").is_ok_and(|s| s.contains("## DATA"));

    let mut out = String::new();
    let scope = if all {
        "all milestones".to_string()
    } else {
        format!("up to {milestone}")
    };
    let _ = writeln!(out, "# Acceptance report ({scope})\n");
    let _ = writeln!(out, "| ID | Milestone | Criterion | Evidence | Status |");
    let _ = writeln!(out, "|----|-----------|-----------|----------|--------|");
    let mut all_green = true;
    for &(id, m, desc) in CRITERIA {
        if !in_scope(m) {
            continue;
        }
        let (evidence, status) = match id {
            "Q1" => {
                let deny = if deny_missing {
                    "deny: not installed (runs in CI)".to_string()
                } else {
                    format!("deny {}", if deny_ok { "ok" } else { "FAILED" })
                };
                let ok = fmt_ok && clippy_ok && (deny_ok || deny_missing);
                (
                    format!(
                        "fmt {}, clippy {}, {deny}",
                        if fmt_ok { "ok" } else { "FAILED" },
                        if clippy_ok { "ok" } else { "FAILED" }
                    ),
                    ok,
                )
            }
            "Q2" => ("docs/WIRE_FORMAT.md".into(), q2_ok),
            _ => match results.get(&id.to_ascii_uppercase()) {
                Some(r) => (
                    format!("{}/{} tests", r.passed.len(), r.passed.len() + r.failed.len()),
                    r.failed.is_empty(),
                ),
                None => {
                    let _ = writeln!(out, "| {id} | {m} | {desc} | no tests yet | ⏳ |");
                    if m == milestone {
                        all_green = false;
                    }
                    continue;
                }
            },
        };
        all_green &= status;
        let _ = writeln!(
            out,
            "| {id} | {m} | {desc} | {evidence} | {} |",
            if status { "✅" } else { "❌" }
        );
    }
    let _ = writeln!(out, "\nAll tests: {}/{} passed.", total.0, total.1);
    let failed: Vec<&String> = results.values().flat_map(|r| &r.failed).collect();
    if !failed.is_empty() {
        let _ = writeln!(out, "\nFailed criterion tests:");
        for f in failed {
            let _ = writeln!(out, "- `{f}`");
        }
    }
    if !clippy_ok {
        let _ = writeln!(out, "\n```\n{}\n```", clippy_out.trim());
    }
    print!("{out}");
    let _ = std::fs::create_dir_all("target");
    let _ = std::fs::write("target/report.md", &out);
    if all_green && tests_ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::criteria_of;

    #[test]
    fn criterion_ids_from_test_names() {
        assert_eq!(criteria_of("r5_timeout"), ["R5"]);
        assert_eq!(criteria_of("wire::tests::b1_data_header"), ["B1"]);
        assert_eq!(criteria_of("pk1_pk2_random"), ["PK1", "PK2"]);
        assert_eq!(criteria_of("p1_pk5_steady"), ["P1", "PK5"]);
        assert!(criteria_of("unreliable_roundtrip").is_empty());
        assert!(criteria_of("rtt_is_measured").is_empty());
    }
}
