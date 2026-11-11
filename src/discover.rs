//! Instance discovery: walk search roots, recognize MAA instance dirs.
//! An instance dir = contains MAA.exe + resource/version.json.
//! All reads here are local file reads; safe while instances run.

use crate::config::Config;
use crate::state::InstanceInfo;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use sysinfo::System;
use walkdir::WalkDir;

pub fn scan(cfg: &Config) -> Vec<InstanceInfo> {
    let mut sys = System::new();
    sys.refresh_processes();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut out = Vec::new();
    // Canonical staging once so we can keep the walker out of our own
    // downloads / MaaResource clone even when staging lives under a search root.
    let staging_canon = std::fs::canonicalize(&cfg.staging_dir).ok();
    for root in &cfg.search_roots {
        let walker = WalkDir::new(root)
            .max_depth(cfg.max_depth)
            .follow_links(false)
            .into_iter()
            .filter_entry(|e| !is_excluded(e.path(), &cfg.staging_dir, staging_canon.as_ref()));
        for entry in walker.filter_map(|e| e.ok()) {
            if !entry.file_type().is_dir() {
                continue;
            }
            let dir = entry.path();
            if is_excluded(dir, &cfg.staging_dir, staging_canon.as_ref()) {
                continue;
            }
            if !dir.join("MAA.exe").is_file() {
                continue;
            }
            if !dir.join("resource").join("version.json").is_file() {
                continue;
            }
            if !seen.insert(canon(dir)) {
                continue;
            }
            out.push(inspect(dir, &sys));
        }
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    out
}

fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// Backup / staging / MAA-updater working dirs are never instances, even if
/// they still contain a full MAA.exe + resource/version.json copy. MAA's own
/// updater keeps replaced files under "<instance>/.old" and extracts pending
/// packages under "<instance>/NewVersionExtract"; launching the exes in there
/// either spawns a zombie duplicate or trips the apphost stub check
/// (Error.Solution.MoveMaaExeOutOfFolder, "请勿直接移动 MAA.exe 文件").
fn is_excluded(dir: &Path, staging: &Path, staging_canon: Option<&PathBuf>) -> bool {
    if !staging.as_os_str().is_empty() && dir.starts_with(staging) {
        return true;
    }
    if let Some(s) = staging_canon {
        // dir may not exist in canonical form yet; best effort only.
        if let Ok(d) = std::fs::canonicalize(dir) {
            if d.starts_with(s) {
                return true;
            }
        }
    }
    dir.components().any(|c| {
        let l = c.as_os_str().to_string_lossy().to_lowercase();
        // MAA reserves exactly ".old" and "NewVersionExtract" inside an instance.
        l == ".old"
            || l == "newversionextract"
            || l.ends_with(".old")
            || l.ends_with(".bak")
            || l.ends_with(".backup")
            || l.ends_with(".tmp")
            || l.ends_with(".orig")
            || l.ends_with('~')
    })
}

fn inspect(dir: &Path, sys: &System) -> InstanceInfo {
    let v = crate::pe_version::read_maa_version(&dir.join("MAA.exe"));
    let cfg_json = read_gui_config(dir);
    InstanceInfo {
        dir: dir.to_path_buf(),
        exe_version_raw: v.raw,
        version_norm: v.normalized.map(|x| x.to_string()),
        channel: channel_of(&cfg_json),
        resource_time: read_resource_time(dir),
        running: is_running(sys, dir),
        pending_package: pending_of(dir, &cfg_json),
    }
}

pub fn strip_bom(s: &str) -> &str {
    s.strip_prefix('\u{FEFF}').unwrap_or(s)
}

fn read_gui_config(dir: &Path) -> serde_json::Value {
    let p = dir.join("config").join("gui.new.json");
    std::fs::read_to_string(p)
        .ok()
        .and_then(|t| serde_json::from_str(strip_bom(&t)).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn channel_of(cfg: &serde_json::Value) -> String {
    let v = &cfg["Update"]["VersionType"];
    if let Some(n) = v.as_u64() {
        return match n {
            0 => "alpha".to_string(),
            1 => "beta".to_string(),
            _ => "stable".to_string(),
        };
    }
    if let Some(s) = v.as_str() {
        let l = s.to_lowercase();
        if l == "nightly" || l == "alpha" || l == "0" {
            return "alpha".to_string();
        }
        if l == "beta" || l == "1" {
            return "beta".to_string();
        }
        return "stable".to_string();
    }
    "stable".to_string()
}

fn pending_of(dir: &Path, cfg: &serde_json::Value) -> Option<String> {
    let name = cfg["Update"]["UpdatePackage"].as_str()?;
    if name.is_empty() {
        return None;
    }
    if dir.join(name).is_file() {
        Some(name.to_string())
    } else {
        None
    }
}

fn read_resource_time(dir: &Path) -> Option<String> {
    let t = std::fs::read_to_string(dir.join("resource").join("version.json")).ok()?;
    let j: serde_json::Value = serde_json::from_str(strip_bom(&t)).ok()?;
    j["last_updated"].as_str().map(|s| s.to_string())
}

pub fn is_running_now(dir: &Path) -> bool {
    let mut sys = System::new();
    sys.refresh_processes();
    is_running(&sys, dir)
}

fn is_running(sys: &System, dir: &Path) -> bool {
    let want = canon(&dir.join("MAA.exe"));
    for p in sys.processes().values() {
        if let Some(exe) = p.exe() {
            if canon(exe) == want {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmp_root(tag: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::SeqCst);
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "maa-fleet-ut-{}-{}-{ms}-{n}",
            std::process::id(),
            tag
        ));
        p
    }

    fn mk_instance(dir: &Path) {
        std::fs::create_dir_all(dir.join("resource")).unwrap();
        std::fs::write(dir.join("MAA.exe"), b"x").unwrap();
        std::fs::write(dir.join("resource").join("version.json"), "{}").unwrap();
    }

    #[test]
    fn skips_backups_and_maa_working_dirs() {
        let root = tmp_root("discover");
        mk_instance(&root.join("inst1"));
        // MAA's own updater working dirs inside a live instance.
        mk_instance(&root.join("inst1").join(".old"));
        mk_instance(&root.join("inst1").join("NewVersionExtract"));
        // Manual whole-folder backups next to live instances.
        mk_instance(&root.join("inst2.old"));
        mk_instance(&root.join("inst2.bak"));
        let cfg = crate::config::Config {
            search_roots: vec![root.clone()],
            max_depth: 5,
            staging_dir: root.join("staging"),
            ..Default::default()
        };
        let found: Vec<PathBuf> = scan(&cfg).into_iter().map(|i| i.dir).collect();
        assert_eq!(found.len(), 1, "unexpected instances: {found:?}");
        assert!(
            found[0].ends_with("inst1"),
            "wrong instance kept: {found:?}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
