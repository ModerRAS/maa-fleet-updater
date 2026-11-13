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
    // An app update replaces the whole installation root: MAA.Updater wipes
    // top-level entries (resource/ included) and rewrites the package over
    // them. If resource/ were a link, that wipe destroys the shared target
    // and every linked instance at once -- exactly the outage this guard
    // exists to prevent. So: app update wins, resource stays a real dir.
    let app_pending = plan
        .items
        .iter()
        .find(|i| i.dir == inst.dir)
        .map(|i| i.kind == "ota" || i.kind == "full")
        .unwrap_or(false);
    // Link mode is disabled unless explicitly opted in: MAA's own updater
    // deletes `resource` on a full package, and a recursive delete through
    // a junction wipes the shared target for every linked instance.
    if cfg.resource_mode == "link" && !cfg.experimental_link_mode {
        emit(
            tx,
            "warn",
            format!(
                "{}: resource_mode=link 但未开启 experimental_link_mode，按 copy 处理（链接模式与 MAA 更新器不兼容）",
                inst.dir.display()
            ),
        );
    }
    let link_wanted = cfg.resource_mode == "link" && cfg.experimental_link_mode && !app_pending;
    if app_pending && crate::resource::is_link(&inst.dir.join("resource")) {
        match crate::resource::unlink_restore(&inst.dir) {
            Ok(how) => {
                rep.note
                    .push_str(&format!("resource {how} (unlinked before app update); "));
                emit(
                    tx,
                    "warn",
                    format!(
                        "{}: 卸载 resource 链接后再装应用包（完整包会删根目录条目）",
                        inst.dir.display()
                    ),
                );
            }
            Err(e) => {
                rep.ok = false;
                rep.note.push_str(&format!("resource unlink failed: {e}; "));
                emit(
                    tx,
                    "error",
                    format!(
                        "{}: 有应用更新但 resource 仍是链接且卸载失败，跳过本次更新: {e}",
                        inst.dir.display()
                    ),
                );
            }
        }
    }
    if !rep.ok {
        return rep;
    }
    if let Some(ct) = central_time {
        let stale_link = link_wanted && !crate::resource::is_linked_to(central_res, &inst.dir);
        // Leaving link mode must always restore a real dir, even when the
        // timestamps already match: an up-to-date linked instance would
        // otherwise keep its link forever and stay one app update away from
        // the same fleet-wide wipe.
        let needs_unlink = !link_wanted && crate::resource::is_link(&inst.dir.join("resource"));
        if older(inst.resource_time.as_deref(), ct) || stale_link || needs_unlink {
            if link_wanted {
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

    fn tmp_root(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "maa-fleet-ut-commit-{}-{tag}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        p
    }

    fn mk_central(root: &Path, stamp: &str) -> PathBuf {
        let central = root.join("staging").join("MaaResource").join("resource");
        std::fs::create_dir_all(central.join("tasks")).unwrap();
        std::fs::write(
            central.join("version.json"),
            format!("{{\"last_updated\":\"{stamp}\"}}"),
        )
        .unwrap();
        std::fs::write(central.join("tasks").join("a.json"), "{}").unwrap();
        central
    }

    fn mk_instance(root: &Path, exe_bytes: &[u8]) -> PathBuf {
        let inst = root.join("inst");
        std::fs::create_dir_all(inst.join("resource")).unwrap();
        std::fs::create_dir_all(inst.join("config")).unwrap();
        std::fs::write(inst.join("MAA.exe"), exe_bytes).unwrap();
        std::fs::write(
            inst.join("resource").join("version.json"),
            r#"{"last_updated":"2020-01-01 00:00:00.000"}"#,
        )
        .unwrap();
        std::fs::write(
            inst.join("config").join("gui.new.json"),
            r#"{"Update":{"Name":"","UpdatePackage":""}}"#,
        )
        .unwrap();
        inst
    }

    fn info_for(dir: &Path) -> InstanceInfo {
        InstanceInfo {
            dir: dir.to_path_buf(),
            exe_version_raw: "v6.13.0".to_string(),
            version_norm: Some("6.13.0".to_string()),
            channel: "stable".to_string(),
            resource_time: Some("2020-01-01 00:00:00.000".to_string()),
            running: false,
            pending_package: None,
            pending_name_raw: None,
            pending_package_raw: None,
            updater_present: true,
            update_failure: None,
            extract_leftover: false,
            update_success: false,
            resource_is_link: false,
        }
    }

    /// The exact outage: a linked instance that then receives a full app
    /// package. MAA's updater wipes root-level entries, so the link must be
    /// dropped (real dir restored) before staging the package.
    #[test]
    fn app_update_unlinks_resource_before_staging() {
        let root = tmp_root("unlink");
        let central = mk_central(&root, "2026-01-01 00:00:00.000");
        let inst_dir = mk_instance(&root, b"old");
        assert_eq!(
            crate::resource::link_into(&central, &inst_dir).unwrap(),
            "linked"
        );
        assert!(crate::resource::is_link(&inst_dir.join("resource")));
        // Package the instance is supposed to install.
        let dl = root.join("staging").join("downloads");
        std::fs::create_dir_all(&dl).unwrap();
        std::fs::write(dl.join("pkg.zip"), make_valid_zip()).unwrap();
        let cfg = Config {
            search_roots: vec![root.clone()],
            staging_dir: root.join("staging"),
            resource_mode: "link".to_string(),
            takeover_autostart: false,
            ..Default::default()
        };
        let plan = Plan {
            items: vec![PlanItem {
                dir: inst_dir.clone(),
                cur_raw: "v6.13.0".to_string(),
                latest: "v6.22.0".to_string(),
                channel: "stable".to_string(),
                kind: "full".to_string(),
                file_name: "pkg.zip".to_string(),
                urls: vec![],
                downloaded: true,
            }],
            resource_time: Some("2026-01-01 00:00:00.000".to_string()),
        };
        let (tx, _rx) = broadcast::channel(64);
        let rep = commit_one(
            &cfg,
            &plan,
            &info_for(&inst_dir),
            &central,
            Some("2026-01-01 00:00:00.000"),
            &tx,
        );
        assert!(rep.ok, "commit failed: {}", rep.note);
        assert!(
            rep.note.contains("unlinked before app update"),
            "guard did not fire: {}",
            rep.note
        );
        assert!(
            !crate::resource::is_link(&inst_dir.join("resource")),
            "resource still a link before app update"
        );
        assert!(inst_dir.join("resource").join("version.json").is_file());
        assert_eq!(rep.app.as_deref(), Some("pkg.zip"));
        // The shared target survived untouched.
        assert!(central.join("version.json").is_file());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// link requested but experimental_link_mode off: behave like copy.
    /// This is the guard that keeps a stale config from re-arming the outage.
    #[test]
    fn link_mode_is_dead_without_opt_in() {
        let root = tmp_root("nolink");
        let stamp = "2026-01-01 00:00:00.000";
        let central = mk_central(&root, stamp);
        let inst_dir = mk_instance(&root, b"exe");
        let cfg = Config {
            search_roots: vec![root.clone()],
            staging_dir: root.join("staging"),
            resource_mode: "link".to_string(),
            experimental_link_mode: false,
            takeover_autostart: false,
            ..Default::default()
        };
        let plan = Plan::default();
        let (tx, _rx) = broadcast::channel(64);
        let rep = commit_one(
            &cfg,
            &plan,
            &info_for(&inst_dir),
            &central,
            Some(stamp),
            &tx,
        );
        assert!(rep.ok, "commit failed: {}", rep.note);
        assert!(
            !crate::resource::is_link(&inst_dir.join("resource")),
            "link mode must stay dead without opt-in: {}",
            rep.note
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Copy mode must reclaim an instance whose resource is still a link even
    /// when its timestamps already match the central repo -- otherwise the
    /// link silently survives the mode switch.
    #[test]
    fn copy_mode_unlinks_even_when_timestamps_match() {
        let root = tmp_root("copyunlink");
        let stamp = "2026-01-01 00:00:00.000";
        let central = mk_central(&root, stamp);
        let inst_dir = mk_instance(&root, b"exe");
        assert_eq!(
            crate::resource::link_into(&central, &inst_dir).unwrap(),
            "linked"
        );
        assert!(crate::resource::is_link(&inst_dir.join("resource")));
        let cfg = Config {
            search_roots: vec![root.clone()],
            staging_dir: root.join("staging"),
            resource_mode: "copy".to_string(),
            takeover_autostart: false,
            ..Default::default()
        };
        let mut info = info_for(&inst_dir);
        // Timestamps equal: the old code skipped the whole resource block.
        info.resource_time = Some(stamp.to_string());
        let plan = Plan::default();
        let (tx, _rx) = broadcast::channel(64);
        let rep = commit_one(&cfg, &plan, &info, &central, Some(stamp), &tx);
        assert!(rep.ok, "commit failed: {}", rep.note);
        assert!(
            !crate::resource::is_link(&inst_dir.join("resource")),
            "link survived copy mode: {}",
            rep.note
        );
        assert!(
            inst_dir
                .join("resource")
                .join("tasks")
                .join("a.json")
                .is_file(),
            "merge did not run after unlink"
        );
        let _ = std::fs::remove_dir_all(&root);
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
