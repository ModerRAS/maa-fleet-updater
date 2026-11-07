//! Central MaaResource git clone + per-instance merge.
//! git pull happens ONLY in the prepare phase. The commit phase only
//! copies already-fetched files (zero network).
//! Merge semantics mirror MAA's DirectoryMerge: overwrite everything,
//! copy version.json LAST so an interrupted merge is retried next boot.

use std::path::{Path, PathBuf};
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
        let dst = instance_dir.join(rel);
        crate::fsutil::ensure_parent(&dst, "resource file")?;
        std::fs::copy(&src, &dst).map_err(|e| format!("copy {}: {e}", rel.display()))?;
        n += 1;
    }
    if let Some(src) = version_file {
        std::fs::copy(&src, instance_dir.join("version.json"))
            .map_err(|e| format!("copy version.json: {e}"))?;
        n += 1;
    }
    let _ = tx;
    Ok(n)
}
