//! Boot-window commit: pure local file operations, ZERO network requests.
//! Runs after reboot while no MAA.exe is up, before any launch.
//! Per instance: resource merge -> app zip stage -> config patch ->
//! failure-flag cleanup -> verify. Launch always follows, even when
//! there was nothing to update or staging failed for someone.

use crate::config::Config;
use crate::state::{emit, InstanceInfo, Plan, PlanItem};
use std::path::{Path, PathBuf};
use tokio::sync::broadcast;

#[derive(Debug, Clone, serde::Serialize)]
pub struct CommitReport {
    pub dir: PathBuf,
    pub ok: bool,
    pub resource_files: usize,
    pub app: Option<String>,
    pub note: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LaunchReport {
    pub dir: PathBuf,
    pub launched: bool,
    pub pid: Option<u32>,
    pub note: String,
}

pub fn commit_all(cfg: &Config, plan: &Plan, tx: &broadcast::Sender<String>) -> Vec<CommitReport> {
    emit(tx, "info", "commit start (local files only, no network)");
    let instances = crate::discover::scan(cfg);
    emit(
        tx,
        "info",
        format!("discovered {} instance(s)", instances.len()),
    );
    let central_res = crate::resource::central_resource_dir(&cfg.staging_dir.join("MaaResource"));
    let central_time = crate::resource::read_last_updated(&central_res);
    emit(
        tx,
        "info",
        format!(
            "commit mode={} central={} central_time={:?}",
            cfg.resource_mode,
            central_res.display(),
            central_time,
        ),
    );
    let mut reports = Vec::new();
    for inst in &instances {
        reports.push(commit_one(
            cfg,
            plan,
            inst,
            &central_res,
            central_time.as_deref(),
            tx,
        ));
    }
    if cfg.takeover_autostart {
        let exes: Vec<PathBuf> = instances.iter().map(|i| i.dir.join("MAA.exe")).collect();
        for m in takeover_autostart(&exes) {
            emit(tx, "info", m);
        }
    }
    let ok = reports.iter().filter(|r| r.ok).count();
    emit(
        tx,
        "info",
        format!("commit done: {ok}/{} ok", reports.len()),
    );
    reports
}

fn older(a: Option<&str>, b: &str) -> bool {
    match a {
        None => true,
        Some(x) => x < b,
    }
}

fn commit_one(
    cfg: &Config,
    plan: &Plan,
    inst: &InstanceInfo,
    central_res: &Path,
    central_time: Option<&str>,
    tx: &broadcast::Sender<String>,
) -> CommitReport {
    let mut rep = CommitReport {
        dir: inst.dir.clone(),
        ok: true,
        resource_files: 0,
        app: None,
        note: String::new(),
    };
    if inst.running {
        rep.ok = false;
        rep.note = "skipped: MAA.exe is running (retry next boot)".to_string();
        emit(tx, "warn", format!("{}: {}", inst.dir.display(), rep.note));
        return rep;
    }
    if let Some(ct) = central_time {
        let stale_link =
            cfg.resource_mode == "link" && !crate::resource::is_linked_to(central_res, &inst.dir);
        if older(inst.resource_time.as_deref(), ct) || stale_link {
            if cfg.resource_mode == "link" {
                match crate::resource::link_into(central_res, &inst.dir) {
                    Ok(how) => {
                        rep.note.push_str(&format!("resource {how}; "));
                    }
                    Err(e) => {
                        // Link creation failed (privilege/FS): fall back to a
                        // copy so the instance still advances; kept visible.
                        match crate::resource::merge_into(central_res, &inst.dir, tx) {
                            Ok(n) => {
                                rep.resource_files = n;
                                rep.note
                                    .push_str(&format!("link failed, copied instead: {e}; "));
                            }
                            Err(e2) => {
                                rep.ok = false;
                                rep.note.push_str(&format!(
                                    "resource link failed: {e}; copy failed: {e2}; "
                                ));
                            }
                        }
                    }
                }
            } else {
                // Leaving link mode: a link must never be merged through
                // (the writes would land in the central repo). Restore first.
                if crate::resource::is_link(&inst.dir.join("resource")) {
                    match crate::resource::unlink_restore(&inst.dir) {
                        Ok(how) => rep.note.push_str(&format!("resource {how}; ")),
                        Err(e) => {
                            rep.ok = false;
                            rep.note.push_str(&format!("resource unlink failed: {e}; "));
                        }
                    }
                }
                if rep.ok {
                    match crate::resource::merge_into(central_res, &inst.dir, tx) {
                        Ok(n) => {
                            rep.resource_files = n;
                        }
                        Err(e) => {
                            rep.ok = false;
                            rep.note.push_str(&format!("resource merge failed: {e}; "));
                        }
                    }
                }
            }
        }
    }
    if let Some(item) = plan.items.iter().find(|i| i.dir == inst.dir) {
        if item.kind == "ota" || item.kind == "full" {
            match stage_app(cfg, inst, item, tx) {
                Ok(name) => {
                    rep.app = Some(name);
                }
                Err(e) => {
                    rep.ok = false;
                    rep.note.push_str(&format!("app stage failed: {e}; "));
                }
            }
        }
    }
    if rep.note.is_empty() {
        rep.note = "ok".to_string();
    }
    emit(
        tx,
        "info",
        format!(
            "{}: resource_files={} app={:?} {}",
            inst.dir.display(),
            rep.resource_files,
            rep.app,
            rep.note
        ),
    );
    rep
}

fn stage_app(
    cfg: &Config,
    inst: &InstanceInfo,
    item: &PlanItem,
    tx: &broadcast::Sender<String>,
) -> Result<String, String> {
    let src = cfg.staging_dir.join("downloads").join(&item.file_name);
    if !src.is_file() {
        return Err(format!("staged file missing: {}", item.file_name));
    }
    // MAA drops a bad package silently (clears UpdatePackage, no flag),
    // and prepare would keep trusting it ("already staged"). Fail loudly
    // here and drop the poisoned staging copy so the next prepare
    // re-downloads instead of re-staging garbage.
    if let Err(e) = check_zip(&src) {
        let _ = std::fs::remove_file(&src);
        let _ = std::fs::remove_file(inst.dir.join(&item.file_name));
        return Err(format!(
            "package corrupt, staging copy removed, rerun prepare: {e}"
        ));
    }
    let dst = inst.dir.join(&item.file_name);
    let need_copy = match (std::fs::metadata(&src), std::fs::metadata(&dst)) {
        (Ok(a), Ok(b)) => a.len() != b.len(),
        _ => true,
    };
    if need_copy {
        std::fs::copy(&src, &dst).map_err(|e| format!("copy package: {e}"))?;
    }
    patch_gui(&inst.dir, &item.latest, &item.file_name)?;
    let flag = inst.dir.join("pending-update-failure.txt");
    if flag.is_file() {
        let _ = std::fs::remove_file(&flag);
    }
    if !dst.is_file() {
        return Err("verify failed: package not in place".to_string());
    }
    emit(
        tx,
        "info",
        format!(
            "{}: staged {} ({})",
            inst.dir.display(),
            item.file_name,
            item.kind
        ),
    );
    Ok(item.file_name.clone())
}

/// Patch config/gui.new.json Update.Name + UpdatePackage.
/// Atomic replace (tmp + rename) + JSON round-trip check; backup kept as .fleetbak.
/// MUST only run while that instance's MAA.exe is down (MAA rewrites this file on save/exit).
fn patch_gui(dir: &Path, version: &str, package: &str) -> Result<(), String> {
    let p = dir.join("config").join("gui.new.json");
    let raw = std::fs::read(&p).map_err(|e| format!("read gui.new.json: {e}"))?;
    let text = String::from_utf8_lossy(&raw);
    let text = crate::discover::strip_bom(&text);
    let mut doc: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("parse gui.new.json: {e}"))?;
    if !doc.is_object() {
        return Err("gui.new.json root is not an object".to_string());
    }
    if !doc["Update"].is_object() {
        doc["Update"] = serde_json::json!({});
    }
    doc["Update"]["Name"] = serde_json::Value::String(version.to_string());
    doc["Update"]["UpdatePackage"] = serde_json::Value::String(package.to_string());
    let bak = dir.join("config").join("gui.new.json.fleetbak");
    std::fs::write(&bak, &raw).map_err(|e| format!("backup config: {e}"))?;
    let out = serde_json::to_string_pretty(&doc).map_err(|e| format!("serialize: {e}"))?;
    serde_json::from_str::<serde_json::Value>(&out)
        .map_err(|e| format!("round-trip check: {e}"))?;
    let tmp = dir.join("config").join("gui.new.json.fleetnew");
    std::fs::write(&tmp, out.as_bytes()).map_err(|e| format!("write tmp: {e}"))?;
    std::fs::rename(&tmp, &p).map_err(|e| format!("replace config: {e}"))?;
    Ok(())
}

/// Structural check only: parses the zip central directory, which catches
/// truncation that still snuck past the download length check (e.g. the
/// server never sent a Content-Length). CRC-checking 300MB on every boot
/// would stall launches; MAA itself re-validates on extract.
fn check_zip(path: &Path) -> Result<(), String> {
    let f = std::fs::File::open(path).map_err(|e| format!("open: {e}"))?;
    let z = zip::ZipArchive::new(f).map_err(|e| format!("not a valid zip: {e}"))?;
    if z.is_empty() {
        return Err("zip has no entries".to_string());
    }
    Ok(())
}

/// One-time (idempotent) takeover: remove MAA_* values under HKCU Run whose
/// target is one of our discovered MAA.exe paths. MAA only writes these on
/// toggle, so removal persists; the in-GUI StartSelf toggle becomes inert.
pub fn takeover_autostart(exes: &[PathBuf]) -> Vec<String> {
    use winreg::{enums::*, RegKey};
    let mut msgs = Vec::new();
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    let run = match hkcu.open_subkey_with_flags(
        "Software\\Microsoft\\Windows\\CurrentVersion\\Run",
        KEY_READ | KEY_WRITE,
    ) {
        Ok(k) => k,
        Err(e) => {
            msgs.push(format!("takeover: cannot open Run key: {e}"));
            return msgs;
        }
    };
    let targets: Vec<String> = exes.iter().map(|p| canon_lower(p)).collect();
    let names: Vec<String> = run
        .enum_values()
        .filter_map(|r| r.ok())
        .map(|(n, _)| n)
        .filter(|n| n.starts_with("MAA_"))
        .collect();
    for n in names {
        let v: String = run.get_value(&n).unwrap_or_default();
        let t = v.trim().trim_matches('"').to_string();
        if targets.iter().any(|x| *x == canon_lower_str(&t)) {
            match run.delete_value(&n) {
                Ok(()) => msgs.push(format!("takeover: removed autostart {n}")),
                Err(e) => msgs.push(format!("takeover: remove {n} failed: {e}")),
            }
        }
    }
    if msgs.is_empty() {
        msgs.push("takeover: no MAA autostart entries".to_string());
    }
    msgs
}

fn canon_lower(p: &Path) -> String {
    std::fs::canonicalize(p)
        .unwrap_or_else(|_| p.to_path_buf())
        .to_string_lossy()
        .to_lowercase()
}

fn canon_lower_str(s: &str) -> String {
    std::fs::canonicalize(s)
        .map(|p| p.to_string_lossy().to_lowercase().to_string())
        .unwrap_or_else(|_| s.to_lowercase())
}

/// Launch every instance that is not already running. Never blocked by
/// update staging results: launching MAA is the primary job.
pub fn launch_all(
    cfg: &Config,
    dirs: &[PathBuf],
    tx: &broadcast::Sender<String>,
) -> Vec<LaunchReport> {
    let mut out = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        if i > 0 && cfg.launch_stagger_secs > 0 {
            std::thread::sleep(std::time::Duration::from_secs(cfg.launch_stagger_secs));
        }
        out.push(launch_one(dir, tx));
    }
    let ok = out.iter().filter(|r| r.launched).count();
    emit(
        tx,
        "info",
        format!("launch done: {ok}/{} launched", out.len()),
    );
    out
}

fn launch_one(dir: &Path, tx: &broadcast::Sender<String>) -> LaunchReport {
    let mut r = LaunchReport {
        dir: dir.to_path_buf(),
        launched: false,
        pid: None,
        note: String::new(),
    };
    if crate::discover::is_running_now(dir) {
        r.note = "already running, skip".to_string();
        return r;
    }
    let exe = dir.join("MAA.exe");
    match std::process::Command::new(&exe)
        .current_dir(dir)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => {
            r.launched = true;
            r.pid = Some(child.id());
            r.note = "launched".to_string();
            emit(
                tx,
                "info",
                format!("{}: launched pid {:?}", dir.display(), r.pid),
            );
        }
        Err(e) => {
            r.note = format!("launch failed: {e}");
            emit(
                tx,
                "error",
                format!("{}: launch failed: {e}", dir.display()),
            );
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_zip(name: &str, bytes: &[u8]) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "maa-fleet-ut-zip-{}-{}-{name}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::write(&p, bytes).unwrap();
        p
    }

    fn make_valid_zip() -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let mut w = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
            w.start_file("a.txt", zip::write::SimpleFileOptions::default())
                .unwrap();
            use std::io::Write;
            w.write_all(b"hello").unwrap();
            w.finish().unwrap();
        }
        buf
    }

    #[test]
    fn rejects_truncated_and_garbage_zips() {
        let good = make_valid_zip();
        let p1 = tmp_zip("good.zip", &good);
        assert!(check_zip(&p1).is_ok(), "valid zip rejected");
        // Truncated: central directory gone.
        let p2 = tmp_zip("cut.zip", &good[..good.len() / 2]);
        assert!(check_zip(&p2).is_err(), "truncated zip accepted");
        // Garbage bytes.
        let p3 = tmp_zip("junk.zip", b"definitely not a zip file at all");
        assert!(check_zip(&p3).is_err(), "garbage accepted");
        // Empty file.
        let p4 = tmp_zip("empty.zip", b"");
        assert!(check_zip(&p4).is_err(), "empty file accepted");
        for p in [&p1, &p2, &p3, &p4] {
            let _ = std::fs::remove_file(p);
        }
    }
}
