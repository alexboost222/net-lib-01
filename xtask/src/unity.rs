//! `cargo xtask unity-package`: assembles the Unity package (UPM) with native plugins and
//! `.meta` files, ready to install from disk or from a tarball.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

const SOURCE: &str = "unity/com.netlib.transport";
const BINDING: &str = "bindings/dotnet/NetLib";
const OUT: &str = "target/unity";

/// A native plugin: file name, folder in `Plugins/`, and its Unity import settings.
struct Plugin {
    /// Folder of the build artifacts (`--natives DIR/<this>/`) and of the plugin in the package.
    platform: &'static str,
    file: &'static str,
    /// `platformData` entries of the PluginImporter.
    importer: &'static str,
}

const EDITOR_OFF: &str = "  - first:\n      Editor: Editor\n    second:\n      enabled: 0\n      settings:\n        DefaultValueInitialized: true\n";

const PLUGINS: &[Plugin] = &[
    Plugin {
        platform: "Windows/x86_64",
        file: "netlib_ffi.dll",
        importer: "  - first:\n      Editor: Editor\n    second:\n      enabled: 1\n      settings:\n        CPU: x86_64\n        DefaultValueInitialized: true\n        OS: Windows\n  - first:\n      Standalone: Win64\n    second:\n      enabled: 1\n      settings:\n        CPU: x86_64\n",
    },
    Plugin {
        platform: "macOS",
        file: "libnetlib_ffi.dylib",
        importer: "  - first:\n      Editor: Editor\n    second:\n      enabled: 1\n      settings:\n        CPU: AnyCPU\n        DefaultValueInitialized: true\n        OS: OSX\n  - first:\n      Standalone: OSXUniversal\n    second:\n      enabled: 1\n      settings:\n        CPU: AnyCPU\n",
    },
    Plugin {
        platform: "Linux/x86_64",
        file: "libnetlib_ffi.so",
        importer: "  - first:\n      Editor: Editor\n    second:\n      enabled: 1\n      settings:\n        CPU: x86_64\n        DefaultValueInitialized: true\n        OS: Linux\n  - first:\n      Standalone: Linux64\n    second:\n      enabled: 1\n      settings:\n        CPU: x86_64\n",
    },
    Plugin {
        platform: "Android/arm64-v8a",
        file: "libnetlib_ffi.so",
        importer: "  - first:\n      Android: Android\n    second:\n      enabled: 1\n      settings:\n        CPU: ARM64\n",
    },
    Plugin {
        platform: "Android/armeabi-v7a",
        file: "libnetlib_ffi.so",
        importer: "  - first:\n      Android: Android\n    second:\n      enabled: 1\n      settings:\n        CPU: ARMv7\n",
    },
    Plugin {
        platform: "Android/x86_64",
        file: "libnetlib_ffi.so",
        importer: "  - first:\n      Android: Android\n    second:\n      enabled: 1\n      settings:\n        CPU: X86_64\n",
    },
    Plugin {
        platform: "iOS",
        file: "libnetlib_ffi.a",
        importer: "  - first:\n      iPhone: iOS\n    second:\n      enabled: 1\n      settings:\n        AddToEmbeddedBinaries: false\n        CPU: ARM64\n        CompileFlags: \n        FrameworkDependencies: \n",
    },
];

/// Deterministic GUID for a package-relative path (two FNV-1a 64 hashes).
pub fn guid(path: &str) -> String {
    let fnv = |seed: u64| {
        path.bytes()
            .fold(seed, |h, b| (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3))
    };
    format!("{:016x}{:016x}", fnv(0xcbf2_9ce4_8422_2325), fnv(0x6c62_272e_07bb_0142))
}

fn meta_tail() -> &'static str {
    "  externalObjects: {}\n  userData: \n  assetBundleName: \n  assetBundleVariant: \n"
}

/// `.meta` content for a file or folder of the package.
pub fn meta(rel: &str, is_dir: bool) -> String {
    let mut m = format!("fileFormatVersion: 2\nguid: {}\n", guid(rel));
    if is_dir {
        m.push_str("folderAsset: yes\nDefaultImporter:\n");
        m.push_str(meta_tail());
        return m;
    }
    if let Some(p) = PLUGINS
        .iter()
        .find(|p| rel == format!("Plugins/{}/{}", p.platform, p.file))
    {
        m.push_str(
            "PluginImporter:\n  externalObjects: {}\n  serializedVersion: 2\n  iconMap: {}\n  executionOrder: {}\n  \
             defineConstraints: []\n  isPreloaded: 0\n  isOverridable: 0\n  isExplicitlyReferenced: 0\n  \
             validateReferences: 1\n  platformData:\n  - first:\n      : Any\n    second:\n      enabled: 0\n      \
             settings:\n        Exclude Android: 1\n        Exclude Editor: 1\n        Exclude Linux64: 1\n        \
             Exclude OSXUniversal: 1\n        Exclude Win64: 1\n        Exclude iOS: 1\n",
        );
        if !p.importer.contains("Editor: Editor") {
            m.push_str(EDITOR_OFF);
        }
        m.push_str(p.importer);
        m.push_str("  userData: \n  assetBundleName: \n  assetBundleVariant: \n");
        return m;
    }
    let importer = match Path::new(rel).extension().and_then(|e| e.to_str()) {
        Some("cs") => {
            "MonoImporter:\n  externalObjects: {}\n  serializedVersion: 2\n  defaultReferences: []\n  \
             executionOrder: 0\n  icon: {instanceID: 0}\n  userData: \n  assetBundleName: \n  assetBundleVariant: \n"
        }
        Some("asmdef") => "AssemblyDefinitionImporter:\n",
        Some("json") if rel == "package.json" => "PackageManifestImporter:\n",
        Some("md" | "txt" | "json") => "TextScriptImporter:\n",
        _ => "DefaultImporter:\n",
    };
    m.push_str(importer);
    if !importer.starts_with("MonoImporter") {
        m.push_str(meta_tail());
    }
    m
}

/// The binding's `Native.cs` resolves functions from the process itself when built with
/// `NETLIB_STATIC`; in Unity that is the iOS player (IL2CPP links the static library).
pub fn patch_native(source: &str) -> String {
    // Line by line: a Windows checkout has CRLF line endings.
    source
        .split_inclusive('\n')
        .map(|line| {
            if line.trim_end() == "#if NETLIB_STATIC" {
                line.replacen(
                    "#if NETLIB_STATIC",
                    "#if NETLIB_STATIC || (UNITY_IOS && !UNITY_EDITOR)",
                    1,
                )
            } else {
                line.to_string()
            }
        })
        .collect()
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let target = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &target)?;
        } else {
            std::fs::copy(e.path(), target)?;
        }
    }
    Ok(())
}

/// Writes a `.meta` next to every file and folder below `dir`, skipping `Samples~`.
fn write_metas(root: &Path, dir: &Path) -> std::io::Result<usize> {
    let mut n = 0;
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.flatten().collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let path = e.path();
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".meta") || name.ends_with('~') || name.starts_with('.') {
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let is_dir = e.file_type()?.is_dir();
        std::fs::write(path.with_file_name(format!("{name}.meta")), meta(&rel, is_dir))?;
        n += 1;
        if is_dir {
            n += write_metas(root, &path)?;
        }
    }
    Ok(n)
}

fn host_platform() -> Option<(&'static str, &'static str)> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => Some(("Windows/x86_64", "netlib_ffi.dll")),
        ("macos", _) => Some(("macOS", "libnetlib_ffi.dylib")),
        ("linux", "x86_64") => Some(("Linux/x86_64", "libnetlib_ffi.so")),
        _ => None,
    }
}

/// Builds the package into `out`. Natives come from `natives/<platform>/<file>`; without a
/// directory, the library is built for the host only.
pub fn build(root: &Path, natives: Option<&Path>, out: &Path) -> Result<Vec<String>, String> {
    let pkg = out.join("com.netlib.transport");
    let _ = std::fs::remove_dir_all(&pkg);
    copy_dir(&root.join(SOURCE), &pkg).map_err(|e| format!("copy package: {e}"))?;
    // Runtime: the binding sources; only Native.cs is adapted for Unity's iOS player.
    for e in std::fs::read_dir(root.join(BINDING))
        .map_err(|e| e.to_string())?
        .flatten()
    {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "cs") {
            let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
            let text = if p.file_name().is_some_and(|n| n == "Native.cs") {
                patch_native(&text)
            } else {
                text
            };
            std::fs::write(pkg.join("Runtime").join(e.file_name()), text).map_err(|e| e.to_string())?;
        }
    }
    let mut included = Vec::new();
    let staged: PathBuf;
    let natives = match natives {
        Some(n) => n,
        None => {
            let (platform, file) = host_platform().ok_or("no --natives and the host is not a desktop platform")?;
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
            let ok = Command::new(cargo)
                .args(["build", "--release", "-p", "netlib-ffi"])
                .status()
                .is_ok_and(|s| s.success());
            if !ok {
                return Err("cargo build --release -p netlib-ffi failed".into());
            }
            staged = out.join("natives");
            std::fs::create_dir_all(staged.join(platform)).map_err(|e| e.to_string())?;
            std::fs::copy(root.join("target/release").join(file), staged.join(platform).join(file))
                .map_err(|e| e.to_string())?;
            &staged
        }
    };
    for p in PLUGINS {
        let from = natives.join(p.platform).join(p.file);
        if from.exists() {
            let dir = pkg.join("Plugins").join(p.platform);
            std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
            std::fs::copy(&from, dir.join(p.file)).map_err(|e| e.to_string())?;
            included.push(format!("{}/{}", p.platform, p.file));
        }
    }
    write_metas(&pkg, &pkg).map_err(|e| format!("metas: {e}"))?;
    Ok(included)
}

pub fn run(args: &[String]) -> Result<String, String> {
    let natives = args
        .windows(2)
        .find(|w| w[0] == "--natives")
        .map(|w| PathBuf::from(&w[1]));
    let root = Path::new(".");
    let out = Path::new(OUT);
    let included = build(root, natives.as_deref(), out)?;
    let version = serde_json::from_str::<serde_json::Value>(
        &std::fs::read_to_string(out.join("com.netlib.transport/package.json")).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?["version"]
        .as_str()
        .unwrap_or("0.0.0")
        .to_string();
    // npm-style tarball: Unity's "Add package from tarball" expects a top-level `package/`.
    let staging = out.join("tar");
    let _ = std::fs::remove_dir_all(&staging);
    copy_dir(&out.join("com.netlib.transport"), &staging.join("package")).map_err(|e| e.to_string())?;
    let tgz = format!("com.netlib.transport-{version}.tgz");
    let tar_ok = Command::new("tar")
        .args(["-czf", &format!("../{tgz}"), "package"])
        .current_dir(&staging)
        .status()
        .is_ok_and(|s| s.success());
    let mut msg = String::new();
    let _ = writeln!(msg, "package: {OUT}/com.netlib.transport");
    if tar_ok {
        let _ = writeln!(msg, "tarball: {OUT}/{tgz}");
    }
    let _ = writeln!(msg, "native plugins ({}):", included.len());
    for i in &included {
        let _ = writeln!(msg, "  {i}");
    }
    let missing: Vec<&str> = PLUGINS
        .iter()
        .map(|p| p.platform)
        .filter(|p| !included.iter().any(|i| i.starts_with(p)))
        .collect();
    if !missing.is_empty() {
        let _ = writeln!(
            msg,
            "missing (build them in CI or pass --natives): {}",
            missing.join(", ")
        );
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn x6_package_is_complete_and_importable() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
        let out = std::env::temp_dir().join(format!("netlib-unity-{}", std::process::id()));
        // Fake natives for every platform.
        let natives = out.join("natives");
        for p in PLUGINS {
            std::fs::create_dir_all(natives.join(p.platform)).unwrap();
            std::fs::write(natives.join(p.platform).join(p.file), b"\x7fELF").unwrap();
        }
        let included = build(&root, Some(&natives), &out).unwrap();
        assert_eq!(included.len(), PLUGINS.len());
        let pkg = out.join("com.netlib.transport");
        // Every file and folder outside Samples~ has a .meta; GUIDs are unique.
        let mut guids = std::collections::HashSet::new();
        let mut stack = vec![pkg.clone()];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap().flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(".meta") || name.ends_with('~') {
                    continue;
                }
                let meta = e.path().with_file_name(format!("{name}.meta"));
                let text = std::fs::read_to_string(&meta).unwrap_or_else(|_| panic!("no meta for {name}"));
                let g = text.lines().nth(1).unwrap().trim_start_matches("guid: ").to_string();
                assert_eq!(g.len(), 32);
                assert!(guids.insert(g), "duplicate guid for {name}");
                if e.file_type().unwrap().is_dir() {
                    stack.push(e.path());
                }
            }
        }
        // The runtime is the binding, with only the iOS switch added.
        for e in std::fs::read_dir(root.join(BINDING)).unwrap().flatten() {
            if e.path().extension().is_some_and(|x| x == "cs") {
                let original = std::fs::read_to_string(e.path()).unwrap();
                let copy = std::fs::read_to_string(pkg.join("Runtime").join(e.file_name())).unwrap();
                if e.file_name() == "Native.cs" {
                    assert!(copy.contains("#if NETLIB_STATIC || (UNITY_IOS && !UNITY_EDITOR)"));
                    assert_eq!(copy.replace(" || (UNITY_IOS && !UNITY_EDITOR)", ""), original);
                } else {
                    assert_eq!(copy, original);
                }
            }
        }
        // Plugin import settings target the right platforms.
        let ios = std::fs::read_to_string(pkg.join("Plugins/iOS/libnetlib_ffi.a.meta")).unwrap();
        assert!(ios.contains("iPhone: iOS") && ios.contains("enabled: 1"));
        let arm = std::fs::read_to_string(pkg.join("Plugins/Android/armeabi-v7a/libnetlib_ffi.so.meta")).unwrap();
        assert!(arm.contains("CPU: ARMv7"));
        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(pkg.join("package.json")).unwrap()).unwrap();
        assert_eq!(manifest["name"], "com.netlib.transport");
        assert_eq!(manifest["unity"], "6000.0");
        let asmdef: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(pkg.join("Runtime/NetLib.asmdef")).unwrap()).unwrap();
        assert_eq!(asmdef["allowUnsafeCode"], true);
        assert_eq!(
            asmdef["noEngineReferences"], true,
            "the runtime does not use UnityEngine"
        );
        let _ = std::fs::remove_dir_all(out);
    }

    #[test]
    fn native_patch_keeps_line_endings() {
        let want = "#if NETLIB_STATIC || (UNITY_IOS && !UNITY_EDITOR)";
        assert_eq!(patch_native("a\n#if NETLIB_STATIC\nb\n"), format!("a\n{want}\nb\n"));
        assert_eq!(patch_native("a\r\n#if NETLIB_STATIC\r\nb"), format!("a\r\n{want}\r\nb"));
        assert_eq!(patch_native("#if NETLIB_STATIC_X\n"), "#if NETLIB_STATIC_X\n");
    }

    #[test]
    fn guids_are_stable() {
        assert_eq!(guid("Runtime"), guid("Runtime"));
        assert_ne!(guid("Runtime"), guid("Runtime/NetLib.asmdef"));
    }
}
