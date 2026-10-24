//! `cargo xtask coverage`: line and branch coverage, .NET coverage, mutation score, and the
//! regression gate against `coverage-baseline.json` (criteria T1-T7).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::process::Command;

use serde_json::{Value, json};

pub const SUMMARY: &str = "target/coverage.json";
const BASELINE: &str = "coverage-baseline.json";

/// Thresholds from docs/PLAN.md, section 3.11.
pub const CORE_LINES: f64 = 90.0;
pub const CORE_BRANCHES: f64 = 80.0;
pub const OTHER_LINES: f64 = 75.0;
pub const DOTNET_LINES: f64 = 80.0;
pub const MUTATION_SCORE: f64 = 85.0;
pub const MAX_DROP: f64 = 0.5;

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".into())
}

fn ok(cmd: &mut Command) -> bool {
    eprintln!("$ {cmd:?}");
    cmd.status().is_ok_and(|s| s.success())
}

/// `(covered, count)` per crate for `metric` ("lines" or "branches") from llvm-cov JSON.
fn per_crate(path: &str, metric: &str) -> BTreeMap<String, (u64, u64)> {
    let mut out = BTreeMap::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return out;
    };
    let Ok(v) = serde_json::from_str::<Value>(&text) else {
        return out;
    };
    for file in v["data"][0]["files"].as_array().into_iter().flatten() {
        let name = file["filename"].as_str().unwrap_or("").replace('\\', "/");
        let Some(krate) = name.split("/crates/").nth(1).and_then(|r| r.split('/').next()) else {
            continue;
        };
        let s = &file["summary"][metric];
        let e = out.entry(krate.to_string()).or_insert((0, 0));
        e.0 += s["covered"].as_u64().unwrap_or(0);
        e.1 += s["count"].as_u64().unwrap_or(0);
    }
    out
}

fn percent((covered, count): (u64, u64)) -> Option<f64> {
    (count > 0).then(|| covered as f64 * 100.0 / count as f64)
}

/// Line rate of the .NET binding from coverlet's Cobertura report.
fn dotnet_lines() -> Option<f64> {
    let dir = "bindings/dotnet/NetLib.Tests/TestResults";
    let _ = std::fs::remove_dir_all(dir);
    let collected = ok(Command::new("dotnet").args([
        "test",
        "bindings/dotnet/NetLib.Tests",
        "--nologo",
        "--collect:XPlat Code Coverage",
    ]));
    if !collected {
        return None;
    }
    let report = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|d| d.path().join("coverage.cobertura.xml"))
        .find(|p| p.exists())?;
    let xml = std::fs::read_to_string(report).ok()?;
    let rate = xml.split("line-rate=\"").nth(1)?.split('"').next()?;
    rate.parse::<f64>().ok().map(|r| r * 100.0)
}

/// Mutation score of `mutants.out/outcomes.json`: caught / (caught + missed + timeout).
pub fn mutation_score() -> Option<(f64, u64, u64)> {
    let text = std::fs::read_to_string("mutants.out/outcomes.json").ok()?;
    let v: Value = serde_json::from_str(&text).ok()?;
    let n = |k: &str| v[k].as_u64().unwrap_or(0);
    let caught = n("caught");
    let failed = n("missed") + n("timeout");
    (caught + failed > 0).then(|| (caught as f64 * 100.0 / (caught + failed) as f64, caught, failed))
}

pub fn run(args: &[String]) -> Result<String, String> {
    let update_baseline = args.iter().any(|a| a == "--update-baseline");
    let html = args.iter().any(|a| a == "--html");
    let require_mutants = args.iter().any(|a| a == "--require-mutants");
    let cargo = cargo();
    let _ = std::fs::create_dir_all("target");
    // Lines: whole workspace on stable.
    let lines_ok = ok(Command::new(&cargo).args([
        "llvm-cov",
        "--workspace",
        "--exclude",
        "xtask",
        "--json",
        "--summary-only",
        "--output-path",
        "target/cov-lines.json",
    ]));
    if !lines_ok {
        return Err("cargo llvm-cov failed (install with `cargo install cargo-llvm-cov`)".into());
    }
    if html {
        ok(Command::new(&cargo).args(["llvm-cov", "report", "--html", "--output-dir", "target/coverage-html"]));
    }
    let lines = per_crate("target/cov-lines.json", "lines");
    // The core by the tests that target it (as for branches): in the workspace run some of
    // its files are instrumented twice, which counts their lines twice.
    let core_ok = ok(Command::new(&cargo).args([
        "llvm-cov",
        "-p",
        "netlib-core",
        "-p",
        "netlib-sim",
        "--json",
        "--summary-only",
        "--output-path",
        "target/cov-core-lines.json",
    ]));
    let core_lines = if core_ok {
        per_crate("target/cov-core-lines.json", "lines")
            .get("netlib-core")
            .copied()
    } else {
        None
    };
    // Branches of the core need nightly.
    let branches_ok = ok(Command::new("rustup")
        .args(["run", "nightly", "cargo", "llvm-cov"])
        .args([
            "--branch",
            "-p",
            "netlib-core",
            "-p",
            "netlib-sim",
            "--json",
            "--summary-only",
            "--output-path",
            "target/cov-branches.json",
        ]));
    let branches = if branches_ok {
        per_crate("target/cov-branches.json", "branches")
    } else {
        BTreeMap::new()
    };
    let dotnet = dotnet_lines();
    let mutants = mutation_score();

    let mut summary = json!({});
    let mut out = String::new();
    let mut failures = Vec::new();
    let _ = writeln!(out, "| Measure | Value | Threshold | |");
    let _ = writeln!(out, "|---------|-------|-----------|--|");
    let mut row = |name: &str, key: &str, value: Option<f64>, min: f64, failures: &mut Vec<String>| {
        let (text, pass) = match value {
            Some(v) => (format!("{v:.1}%"), v >= min),
            None => ("not measured".to_string(), false),
        };
        let _ = writeln!(
            out,
            "| {name} | {text} | ≥ {min}% | {} |",
            if pass { "✅" } else { "❌" }
        );
        if !pass {
            failures.push(format!("{name}: {text} < {min}%"));
        }
        if let Some(v) = value {
            summary[key] = json!((v * 10.0).round() / 10.0);
        }
    };
    row(
        "T1 netlib-core lines",
        "core_lines",
        core_lines.and_then(percent),
        CORE_LINES,
        &mut failures,
    );
    row(
        "T2 netlib-core branches (nightly)",
        "core_branches",
        branches.get("netlib-core").copied().and_then(percent),
        CORE_BRANCHES,
        &mut failures,
    );
    for krate in ["netlib", "netlib-ffi", "netlib-log", "netlib-sim"] {
        row(
            &format!("T3 {krate} lines"),
            &format!("{krate}_lines"),
            lines.get(krate).copied().and_then(percent),
            OTHER_LINES,
            &mut failures,
        );
    }
    // Mutation testing takes long and runs in its own (nightly) job; it counts here only
    // when its results are present.
    if mutants.is_some() || require_mutants {
        row(
            "T4 netlib-core mutation score",
            "core_mutation_score",
            mutants.map(|m| m.0),
            MUTATION_SCORE,
            &mut failures,
        );
    }
    row(
        "T5 .NET binding lines",
        "dotnet_lines",
        dotnet,
        DOTNET_LINES,
        &mut failures,
    );

    // T7: no measure may drop by more than MAX_DROP points against the baseline.
    let baseline: Value = std::fs::read_to_string(BASELINE)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(json!({}));
    let _ = writeln!(out, "\nT7 against {BASELINE}:");
    for (k, v) in summary.as_object().into_iter().flatten() {
        let now = v.as_f64().unwrap_or(0.0);
        if let Some(base) = baseline[k].as_f64() {
            let delta = now - base;
            let bad = delta < -MAX_DROP;
            let _ = writeln!(
                out,
                "- {k}: {base:.1} -> {now:.1} ({delta:+.1}){}",
                if bad { " ❌" } else { "" }
            );
            if bad {
                failures.push(format!("{k} dropped by {:.1} points", -delta));
            }
        }
    }
    if let Some((score, caught, missed)) = mutants {
        let _ = writeln!(
            out,
            "\nmutants: {caught} caught, {missed} missed or timed out ({score:.1}%)"
        );
    }
    let _ = std::fs::write(SUMMARY, serde_json::to_string_pretty(&summary).unwrap_or_default());
    if update_baseline {
        let _ = std::fs::write(
            BASELINE,
            serde_json::to_string_pretty(&summary).unwrap_or_default() + "\n",
        );
        let _ = writeln!(out, "\nbaseline updated");
    }
    if failures.is_empty() {
        Ok(out)
    } else {
        Err(format!("{out}\nfailed:\n- {}", failures.join("\n- ")))
    }
}
