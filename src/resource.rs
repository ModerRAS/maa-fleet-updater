//! Central MaaResource git clone + per-instance merge.
//! git pull happens ONLY in the prepare phase. The commit phase only
//! copies already-fetched files (zero network).
//! Merge semantics mirror MAA's DirectoryMerge: overwrite everything,
//! copy version.json LAST so an interrupted merge is retried next boot.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use walkdir::WalkDir;

fn git_args(proxy: &str) -> Vec<String> {
    // Fail fast on half-dead connections instead of hanging the prepare phase.
    let mut a = vec![
        "-c".to_string(),
        "http.lowSpeedLimit=1024".to_string(),
        "-c".to_string(),
        "http.lowSpeedTime=60".to_string(),
    ];
    if !proxy.trim().is_empty() {
        a.push("-c".to_string());
        a.push(format!("http.proxy={}", proxy.trim()));
        a.push("-c".to_string());
        a.push(format!("https.proxy={}", proxy.trim()));
    }
    a
}

async fn git(dir: Option<&Path>, proxy: &str, args: &[&str]) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("git");
    for a in git_args(proxy) {
        cmd.arg(a);
    }
    if let Some(d) = dir {
        cmd.current_dir(d);
    }
    cmd.args(args);
    let out = cmd.output().await.map_err(|e| format!("spawn git: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

pub async fn ensure_repo(
    repo_dir: &Path,
    url: &str,
    branch: &str,
    proxy: &str,
) -> Result<(), String> {
    if repo_dir.join(".git").is_dir() {
        return Ok(());
    }
    crate::fsutil::ensure_parent(repo_dir, "resource repo")?;
    git(
        None,
        proxy,
        &[
            "clone",
            "--depth",
            "1",
            "-b",
            branch,
            url,
            &repo_dir.to_string_lossy(),
        ],
    )
    .await
    .map(|_| ())
}

pub async fn update_repo(repo_dir: &Path, branch: &str, proxy: &str) -> Result<String, String> {
    git(
        Some(repo_dir),
        proxy,
        &["fetch", "--depth", "1", "origin", branch],
    )
    .await?;
    git(Some(repo_dir), proxy, &["reset", "--hard", "FETCH_HEAD"]).await?;
    git(Some(repo_dir), proxy, &["rev-parse", "--short", "HEAD"]).await
}

pub fn central_resource_dir(repo_dir: &Path) -> PathBuf {
    repo_dir.join("resource")
}

pub fn read_last_updated(resource_dir: &Path) -> Option<String> {
    let t = std::fs::read_to_string(resource_dir.join("version.json")).ok()?;
    let j: serde_json::Value = serde_json::from_str(&t).ok()?;
    j["last_updated"].as_str().map(|s| s.to_string())
}

/// Merge central resource/ into an instance dir. Returns files copied.
/// version.json is copied last (same guarantee as MAA's DirectoryMerge).
pub fn merge_into(
    central_res: &Path,
    instance_dir: &Path,
    tx: &broadcast::Sender<String>,
) -> Result<usize, String> {
    // A link must never be merged through: the destination would resolve
    // into the central repo and pollute it. Unlink first (copy mode does).
    if std::fs::read_link(instance_dir.join("resource")).is_ok() {
        return Err(
            "instance resource is a link; unlink first (copy mode restores the real dir)"
                .to_string(),
        );
    }
    let mut files: Vec<PathBuf> = Vec::new();
    for e in WalkDir::new(central_res)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if e.file_type().is_file() {
            files.push(e.path().to_path_buf());
        }
    }
    let mut version_file: Option<PathBuf> = None;
    let mut n = 0usize;
    for src in files {
        let rel = src
            .strip_prefix(central_res)
            .map_err(|e| format!("relpath: {e}"))?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        if rel
            .file_name()
            .map(|n| n == "version.json")
            .unwrap_or(false)
            && rel
                .parent()
                .map(|p| p.as_os_str().is_empty())
                .unwrap_or(false)
        {
            version_file = Some(src);
            continue;
        }
        let dst = instance_dir.join("resource").join(rel);
        crate::fsutil::ensure_parent(&dst, "resource file")?;
        std::fs::copy(&src, &dst).map_err(|e| format!("copy {}: {e}", rel.display()))?;
        n += 1;
    }
    if let Some(src) = version_file {
        std::fs::copy(&src, instance_dir.join("resource").join("version.json"))
            .map_err(|e| format!("copy version.json: {e}"))?;
        n += 1;
    }
    let _ = tx;
    Ok(n)
}

// ---- Link mode: <instance>/resource -> central resource ----
// Zero-copy fleet sync: one `git pull` in staging, every linked instance
// sees it instantly. Discovery, version checks and the MAA updater flow
// are unchanged (all of them follow the link transparently).

/// True when `path` is itself a symlink/junction (never follows it).
pub fn is_link(path: &Path) -> bool {
    std::fs::read_link(path).is_ok()
}

fn canon(p: &Path) -> PathBuf {
    std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf())
}

/// True when <instance>/resource is a link pointing at this central dir.
/// Canonical comparison so staging moves / case differences don't fool it.
pub fn is_linked_to(central_res: &Path, instance_dir: &Path) -> bool {
    let link = instance_dir.join("resource");
    let target = match std::fs::read_link(&link) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let abs = if target.is_absolute() {
        target
    } else {
        link.parent().unwrap_or(Path::new(".")).join(target)
    };
    canon(&abs) == canon(central_res)
}

/// Replace <instance>/resource with a link to the central resource dir.
/// A pre-existing real dir is preserved as resource.fleetbak.<ms>.
/// Returns "linked" or "already-linked" for status notes.
pub fn link_into(central_res: &Path, instance_dir: &Path) -> Result<&'static str, String> {
    let target = central_res
        .canonicalize()
        .map_err(|e| format!("central resource missing {}: {e}", central_res.display()))?;
    if !target.join("version.json").is_file() {
        return Err(format!(
            "central resource has no version.json: {}",
            target.display()
        ));
    }
    let link = instance_dir.join("resource");
    if std::fs::read_link(&link).is_ok() {
        if is_linked_to(central_res, instance_dir) {
            return Ok("already-linked");
        }
        remove_link(&link)?;
    } else if link.exists() {
        let bak = backup_path(instance_dir);
        std::fs::rename(&link, &bak).map_err(|e| format!("backup {}: {e}", link.display()))?;
    }
    create_link(&target, &link)?;
    if !link.join("version.json").is_file() {
        return Err(format!("link verify failed: {}", link.display()));
    }
    Ok("linked")
}

/// Remove the resource link and restore the newest resource.fleetbak.*
/// backup (or recreate an empty dir when there is none, e.g. link mode
/// was on from the first run). Used when switching back to copy mode.
pub fn unlink_restore(instance_dir: &Path) -> Result<&'static str, String> {
    let link = instance_dir.join("resource");
    if std::fs::read_link(&link).is_ok() {
        remove_link(&link)?;
    }
    let mut baks: Vec<PathBuf> = std::fs::read_dir(instance_dir)
        .map_err(|e| format!("read {}: {e}", instance_dir.display()))?
        .filter_map(|e| e.ok().map(|x| x.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("resource.fleetbak."))
                .unwrap_or(false)
        })
        .collect();
    baks.sort();
    match baks.pop() {
        Some(b) => {
            std::fs::rename(&b, &link).map_err(|e| format!("restore backup: {e}"))?;
            Ok("restored")
        }
        None => {
            std::fs::create_dir_all(&link).map_err(|e| format!("recreate resource: {e}"))?;
            Ok("recreated-empty")
        }
    }
}

/// Remove the link itself, never the target. Dir-symlinks and NTFS
/// junctions both go away with remove_dir; file-symlinks with remove_file.
/// A non-empty real dir fails both ways, which is the safety we want:
/// callers must rename it aside, never delete through this helper.
fn remove_link(link: &Path) -> Result<(), String> {
    match std::fs::remove_dir(link) {
        Ok(()) => Ok(()),
        Err(_) => {
            std::fs::remove_file(link).map_err(|e| format!("remove link {}: {e}", link.display()))
        }
    }
}

fn backup_path(instance_dir: &Path) -> PathBuf {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    instance_dir.join(format!("resource.fleetbak.{ms}"))
}

#[cfg(windows)]
fn create_link(target: &Path, link: &Path) -> Result<(), String> {
    if std::os::windows::fs::symlink_dir(target, link).is_ok() {
        return Ok(());
    }
    // No developer mode / privilege: fall back to an NTFS junction,
    // creatable by normal users. mklink wants absolute paths.
    let link_s = link.to_string_lossy().into_owned();
    let target_s = target.to_string_lossy().into_owned();
    let out = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J", link_s.as_str(), target_s.as_str()])
        .output()
        .map_err(|e| format!("mklink spawn: {e}"))?;
    if out.status.success() && link.join("version.json").is_file() {
        Ok(())
    } else {
        Err(format!(
            "mklink /J failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

#[cfg(not(windows))]
fn create_link(target: &Path, link: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(target, link).map_err(|e| format!("symlink: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_targets_resource_subdir() {
        let mut root = std::env::temp_dir();
        root.push(format!(
            "maa-fleet-ut-res-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        // central_res == <repo>/resource
        let central = root.join("central");
        std::fs::create_dir_all(central.join("tasks")).unwrap();
        std::fs::write(
            central.join("version.json"),
            r#"{"last_updated":"2026-01-01 00:00:00.000"}"#,
        )
        .unwrap();
        std::fs::write(central.join("tasks").join("a.json"), "{}").unwrap();
        let inst = root.join("inst");
        std::fs::create_dir_all(inst.join("resource")).unwrap();
        std::fs::write(
            inst.join("resource").join("version.json"),
            r#"{"last_updated":"2020-01-01 00:00:00.000"}"#,
        )
        .unwrap();
        let (tx, _rx) = broadcast::channel(16);
        let n = merge_into(&central, &inst, &tx).unwrap();
        assert_eq!(n, 2, "expected payload + version.json");
        assert!(
            inst.join("resource").join("tasks").join("a.json").is_file(),
            "payload must land under resource/"
        );
        assert!(
            !inst.join("tasks").join("a.json").exists(),
            "must not pollute instance root"
        );
        assert!(
            !inst.join("version.json").exists(),
            "version.json must not be copied to instance root"
        );
        let updated = std::fs::read_to_string(inst.join("resource").join("version.json")).unwrap();
        assert!(
            updated.contains("2026"),
            "resource/version.json not advanced: {updated}"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let mut p = std::env::temp_dir();
        p.push(format!(
            "maa-fleet-ut-link-{}-{}-{}",
            std::process::id(),
            tag,
            ms
        ));
        p
    }

    fn mk_central(root: &Path, stamp: &str) -> PathBuf {
        let central = root.join("central").join("resource");
        std::fs::create_dir_all(central.join("tasks")).unwrap();
        std::fs::write(
            central.join("version.json"),
            format!("{{\"last_updated\":\"{stamp}\"}}"),
        )
        .unwrap();
        std::fs::write(central.join("tasks").join("a.json"), "{}").unwrap();
        central
    }

    fn mk_old_instance(root: &Path) -> PathBuf {
        let inst = root.join("inst");
        std::fs::create_dir_all(inst.join("resource")).unwrap();
        std::fs::write(
            inst.join("resource").join("version.json"),
            r#"{"last_updated":"2020-01-01 00:00:00.000"}"#,
        )
        .unwrap();
        std::fs::write(inst.join("resource").join("stale.txt"), "old").unwrap();
        inst
    }

    fn backups(inst: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(inst)
            .unwrap()
            .filter_map(|e| e.ok().map(|x| x.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("resource.fleetbak."))
                    .unwrap_or(false)
            })
            .collect()
    }

    #[test]
    fn link_replaces_dir_serves_files_and_is_idempotent() {
        let root = tmp_root("replace");
        let central = mk_central(&root, "2026-01-01 00:00:00.000");
        let inst = mk_old_instance(&root);
        assert_eq!(super::link_into(&central, &inst).unwrap(), "linked");
        // Serves central content through the link.
        let v = std::fs::read_to_string(inst.join("resource").join("version.json")).unwrap();
        assert!(v.contains("2026"), "unexpected: {v}");
        assert!(inst.join("resource").join("tasks").join("a.json").is_file());
        // Old real dir preserved aside; stale file went with it, invisible now.
        let baks = backups(&inst);
        assert_eq!(baks.len(), 1, "backups: {baks:?}");
        assert!(baks[0].join("stale.txt").is_file());
        assert!(!inst.join("resource").join("stale.txt").exists());
        // Second run is a no-op.
        assert_eq!(super::link_into(&central, &inst).unwrap(), "already-linked");
        assert!(super::is_linked_to(&central, &inst));
        // Central moves forward: visible instantly, zero copy.
        std::fs::write(
            central.join("version.json"),
            r#"{"last_updated":"2027-01-01 00:00:00.000"}"#,
        )
        .unwrap();
        let v2 = std::fs::read_to_string(inst.join("resource").join("version.json")).unwrap();
        assert!(v2.contains("2027"), "unexpected: {v2}");
        // Switching back to copy mode restores the real dir.
        assert_eq!(super::unlink_restore(&inst).unwrap(), "restored");
        assert!(!super::is_link(&inst.join("resource")));
        let v3 = std::fs::read_to_string(inst.join("resource").join("version.json")).unwrap();
        assert!(v3.contains("2020"), "backup not restored: {v3}");
        // And the classic merge works again on the restored real dir.
        let (tx, _rx) = broadcast::channel(16);
        let n = super::merge_into(&central, &inst, &tx).unwrap();
        assert!(n >= 2, "merged {n}");
        let v4 = std::fs::read_to_string(inst.join("resource").join("version.json")).unwrap();
        assert!(v4.contains("2027"), "unexpected: {v4}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn link_refuses_bare_central_and_merge_refuses_link() {
        let root = tmp_root("refuse");
        // Central without version.json: refuse, instance untouched.
        let bare = root.join("bare").join("resource");
        std::fs::create_dir_all(&bare).unwrap();
        let inst = mk_old_instance(&root);
        assert!(super::link_into(&bare, &inst).is_err());
        assert!(!super::is_link(&inst.join("resource")));
        // Healthy link: merge must refuse to write through it.
        let central = mk_central(&root, "2026-06-01 00:00:00.000");
        assert_eq!(super::link_into(&central, &inst).unwrap(), "linked");
        let (tx, _rx) = broadcast::channel(16);
        let err = super::merge_into(&central, &inst, &tx).unwrap_err();
        assert!(err.contains("link"), "unexpected: {err}");
        // A central dir that was never linked is not reported as linked.
        let gone = root.join("gone").join("resource");
        assert!(!super::is_linked_to(&gone, &inst));
        let _ = std::fs::remove_dir_all(&root);
    }
}
