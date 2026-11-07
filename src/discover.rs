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
    for root in &cfg.search_roots {
        let walker = WalkDir::new(root)
            .max_depth(cfg.max_depth)
            .follow_links(false)
            .into_iter();
        for entry in walker.filter_map(|e| e.ok()) {
            if !entry.file_type().is_dir() { continue; }
            let dir = entry.path();
            if !dir.join("MAA.exe").is_file() { continue; }
            if !dir.join("resource").join("version.json").is_file() { continue; }
            if !seen.insert(canon(dir)) { continue; }
            out.push(inspect(dir, &sys));
        }
    }
    out.sort_by(|a, b| a.dir.cmp(&b.dir));
    out
}

fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
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
        if l == "nightly" || l == "alpha" || l == "0" { return "alpha".to_string(); }
        if l == "beta" || l == "1" { return "beta".to_string(); }
        return "stable".to_string();
    }
    "stable".to_string()
}

fn pending_of(dir: &Path, cfg: &serde_json::Value) -> Option<String> {
    let name = cfg["Update"]["UpdatePackage"].as_str()?;
    if name.is_empty() { return None; }
    if dir.join(name).is_file() { Some(name.to_string()) } else { None }
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
    for (_, p) in sys.processes() {
        if let Some(exe) = p.exe() {
            if canon(exe) == want { return true; }
        }
    }
    false
}