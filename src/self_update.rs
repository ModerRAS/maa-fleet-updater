//! Self update for this daemon: check the GitHub Release feed, download the
//! matching archive + SHA256, verify, extract only the exe, swap it in via a
//! short-lived replacer child process, and restart with the same --config.
//!
//! Mechanics mirror arknights-auto-update's app-self-update (semver + commit
//! comparison so movable tags still trigger; .new staging; Windows rename of
//! the running exe; stdin-EOF handoff), trimmed to this daemon's needs.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::broadcast;

pub const DEFAULT_FEED: &str =
    "https://api.github.com/repos/ModerRAS/maa-fleet-updater/releases/latest";
/// Static manifest: same Release JSON subset, fetched through the release-asset
/// CDN so routine checks consume zero REST API quota. The API stays as fallback
/// for transition releases that predate the manifest.
pub const MANIFEST_URL: &str =
    "https://github.com/ModerRAS/maa-fleet-updater/releases/latest/download/latest.json";
pub const ASSET_ZIP: &str = "maa-fleet-updater-win-x64.zip";
pub const EXE_NAME: &str = "maa-fleet-updater.exe";

const MAX_ARCHIVE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BINARY_BYTES: u64 = 80 * 1024 * 1024;
const MAX_TEXT_BYTES: u64 = 1024 * 1024;
const MAX_ENTRIES: usize = 64;

pub fn current_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub fn current_commit() -> Option<&'static str> {
    option_env!("MAA_FLEET_GIT_SHA")
        .map(str::trim)
        .filter(|v| looks_like_commit(v))
}

/// Release builds live next to config.json; dev builds live under target/.
pub fn running_from_cargo_target() -> bool {
    let Ok(path) = std::env::current_exe() else {
        return false;
    };
    let text = path
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase();
    text.contains("/target/debug/") || text.contains("/target/release/")
}

/// Restart keeps the same config file; nothing else needs preserving.
pub fn restart_args_for(cfg_path: &Path) -> Vec<String> {
    vec![
        "--config".to_string(),
        cfg_path.to_string_lossy().into_owned(),
    ]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteAsset {
    pub name: String,
    pub url: String,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRelease {
    pub tag: String,
    pub version: String,
    pub commit: Option<String>,
    pub assets: Vec<RemoteAsset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedDownload {
    pub version: String,
    pub commit: Option<String>,
    pub archive: RemoteAsset,
    pub checksum: RemoteAsset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateDecision {
    Keep { message: String },
    Update(PlannedDownload),
}

#[derive(Debug, Clone, serde::Serialize, Default)]
pub struct SelfUpdateState {
    pub current_version: String,
    pub current_commit: Option<String>,
    pub last_check_at: Option<u64>,
    pub latest: Option<String>,
    pub latest_commit: Option<String>,
    pub decision: String,
    pub note: String,
}

#[derive(Debug)]
pub enum StageOutcome {
    UpToDate { message: String },
    Staged { path: PathBuf, version: String },
}

pub struct StageRequest<'a> {
    pub client: &'a reqwest::Client,
    pub feed: &'a str,
    pub work_dir: &'a Path,
    pub install_exe: &'a Path,
    pub events: Option<broadcast::Sender<String>>,
}

fn emit(events: &Option<broadcast::Sender<String>>, level: &str, msg: String) {
    if let Some(tx) = events {
        crate::state::emit(tx, level, msg);
    }
}

/// Strict vX.Y.Z tags only; moving tags are distinguished by commit instead.
pub fn parse_semver(text: &str) -> Result<(u64, u64, u64), String> {
    let raw = text.trim().trim_start_matches('v');
    let mut parts = raw.split('.');
    let major = parse_part(parts.next(), text)?;
    let minor = parse_part(parts.next(), text)?;
    let patch = parse_part(parts.next(), text)?;
    if parts.next().is_some() {
        return Err(format!("版本号需要是 MAJOR.MINOR.PATCH: {text}"));
    }
    Ok((major, minor, patch))
}

fn parse_part(part: Option<&str>, raw: &str) -> Result<u64, String> {
    part.filter(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
        .and_then(|p| p.parse::<u64>().ok())
        .ok_or_else(|| format!("版本号需要是 MAJOR.MINOR.PATCH: {raw}"))
}

pub fn looks_like_commit(s: &str) -> bool {
    let s = s.trim();
    (7..=64).contains(&s.len()) && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn commits_differ(a: &str, b: &str) -> bool {
    !(a == b || a.starts_with(b) || b.starts_with(a))
}

pub fn decide_update(
    current_version: &str,
    current_commit: Option<&str>,
    remote: &RemoteRelease,
) -> Result<UpdateDecision, String> {
    let local = parse_semver(current_version)?;
    let remote_version = parse_semver(&remote.version)?;
    if remote_version < local {
        return Ok(UpdateDecision::Keep {
            message: format!(
                "远端 {} 比当前 {} 旧，保持现在的程序。",
                remote.version, current_version
            ),
        });
    }
    if remote_version == local {
        match (current_commit.map(str::trim), remote.commit.as_deref()) {
            (Some(lc), Some(rc))
                if looks_like_commit(lc) && looks_like_commit(rc) && !commits_differ(lc, rc) =>
            {
                return Ok(UpdateDecision::Keep {
                    message: format!("程序已是发布 {}。", remote.version),
                });
            }
            (Some(_), Some(_)) => {}
            _ => {
                return Ok(UpdateDecision::Keep {
                    message: format!(
                        "程序版本已经是 {}。没有可对照的提交号，所以不重复下载。",
                        remote.version
                    ),
                });
            }
        }
    }
    let download = select_download(remote)?;
    Ok(UpdateDecision::Update(download))
}

fn select_download(remote: &RemoteRelease) -> Result<PlannedDownload, String> {
    let archive = remote
        .assets
        .iter()
        .find(|a| a.name == ASSET_ZIP)
        .cloned()
        .ok_or_else(|| format!("当前平台没有对应的发布包: {ASSET_ZIP}"))?;
    let checksum_name = format!("{ASSET_ZIP}.sha256");
    let checksum = remote
        .assets
        .iter()
        .find(|a| a.name == checksum_name)
        .cloned()
        .ok_or_else(|| format!("发布里没有 {checksum_name}"))?;
    Ok(PlannedDownload {
        version: remote.version.clone(),
        commit: remote.commit.clone(),
        archive,
        checksum,
    })
}

/// Proxy-aware client that does NOT follow redirects on its own: every hop is
/// checked against the feed allowlist first (same rule as downloads).
pub fn build_client(proxy: &str) -> Result<reqwest::Client, String> {
    let mut b = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none());
    if !proxy.trim().is_empty() {
        let p = reqwest::Proxy::all(proxy.trim()).map_err(|e| format!("bad proxy: {e}"))?;
        b = b.proxy(p);
    }
    b.build().map_err(|e| format!("build http client: {e}"))
}

/// Empty feed = official flow: static manifest first, REST API as fallback.
/// A custom feed is tried alone (its owner controls the source).
async fn fetch_best(
    client: &reqwest::Client,
    feed: &str,
    current_version: Option<&str>,
) -> Result<RemoteRelease, String> {
    let feeds: Vec<String> = if feed.trim().is_empty() {
        vec![MANIFEST_URL.to_string(), DEFAULT_FEED.to_string()]
    } else {
        vec![feed.to_string()]
    };
    let mut errors = Vec::new();
    for f in &feeds {
        match fetch_release(client, f, current_version).await {
            Ok(r) => return Ok(r),
            Err(e) => errors.push(format!("{f}: {e}")),
        }
    }
    Err(errors.join("; ".into()))
}

async fn fetch_release(
    client: &reqwest::Client,
    feed: &str,
    current_version: Option<&str>,
) -> Result<RemoteRelease, String> {
    let text = get_text(client, feed, feed, MAX_TEXT_BYTES).await?;
    let document: ReleaseDocument =
        serde_json::from_str(&text).map_err(|e| format!("发布信息无法解析: {e}"))?;
    let version = document.tag_name.trim().trim_start_matches('v').to_string();
    parse_semver(&version)?;
    let mut commit = document
        .commit
        .as_deref()
        .filter(|v| looks_like_commit(v))
        .map(ToOwned::to_owned)
        .or_else(|| {
            document
                .target_commitish
                .as_deref()
                .filter(|v| looks_like_commit(v))
                .map(ToOwned::to_owned)
        });
    // The commit only matters when versions tie (moved-tag detection);
    // skip the extra tag-ref API call otherwise to save rate quota.
    let need_commit = current_version
        .map(|c| c.trim().trim_start_matches('v') == version)
        .unwrap_or(true);
    if commit.is_none() && need_commit {
        if let Some((owner, repo)) = github_repo(feed) {
            commit = fetch_tag_commit(client, &owner, &repo, &document.tag_name)
                .await
                .ok();
        }
    }
    Ok(RemoteRelease {
        tag: document.tag_name,
        version,
        commit,
        assets: document
            .assets
            .into_iter()
            .map(|a| RemoteAsset {
                name: a.name,
                url: a.browser_download_url,
                size: a.size,
            })
            .collect(),
    })
}

pub async fn stage_release(request: &StageRequest<'_>) -> Result<StageOutcome, String> {
    let remote = fetch_best(request.client, request.feed, Some(current_version())).await?;
    match decide_update(current_version(), current_commit(), &remote)? {
        UpdateDecision::Keep { message } => Ok(StageOutcome::UpToDate { message }),
        UpdateDecision::Update(plan) => {
            let staged = download_and_stage(request, &plan).await?;
            Ok(StageOutcome::Staged {
                path: staged,
                version: plan.version,
            })
        }
    }
}

async fn download_and_stage(
    request: &StageRequest<'_>,
    plan: &PlannedDownload,
) -> Result<PathBuf, String> {
    ensure_asset_url(request.feed, &plan.archive.url)?;
    ensure_asset_url(request.feed, &plan.checksum.url)?;
    std::fs::create_dir_all(request.work_dir).map_err(|e| format!("创建自更新目录失败: {e}"))?;
    let staged = staged_path(request.install_exe);
    let staged_cleanup = staged.clone();
    let _ = std::fs::remove_file(&staged);
    emit(
        &request.events,
        "info",
        format!("自更新: 下载 {} ...", plan.archive.name),
    );
    let checksum_text = get_text(
        request.client,
        &plan.checksum.url,
        request.feed,
        MAX_TEXT_BYTES,
    )
    .await?;
    let expected = parse_sha256_file(&checksum_text)?;
    let archive_path = request.work_dir.join(safe_file_name(&plan.archive.name)?);
    let part_path = archive_path.with_extension("part");
    let outcome = async {
        download_file(
            request.client,
            &plan.archive.url,
            request.feed,
            &part_path,
            MAX_ARCHIVE_BYTES,
        )
        .await?;
        emit(
            &request.events,
            "info",
            "自更新: 校验 SHA256 ...".to_string(),
        );
        let actual = tokio::task::spawn_blocking({
            let p = part_path.clone();
            move || sha256_file(&p)
        })
        .await
        .map_err(|e| format!("校验任务中断: {e}"))??;
        if actual != expected {
            return Err("程序包 SHA256 不一致".to_string());
        }
        std::fs::rename(&part_path, &archive_path).map_err(|e| format!("保存程序包失败: {e}"))?;
        emit(
            &request.events,
            "info",
            "自更新: 解出新程序 ...".to_string(),
        );
        let ap = archive_path.clone();
        let st = staged.clone();
        tokio::task::spawn_blocking(move || extract_binary(&ap, &st))
            .await
            .map_err(|e| format!("解压任务中断: {e}"))??;
        Ok(staged)
    }
    .await;
    if outcome.is_err() {
        let _ = std::fs::remove_file(&part_path);
        let _ = std::fs::remove_file(&staged_cleanup);
    }
    outcome
}

/// Swap staged .new into place. Windows renames the running exe aside first
/// (running images can be renamed, not deleted); rollback on failure.
pub fn swap_executable(from: &Path, to: &Path) -> Result<(), String> {
    validate_swap_paths(from, to)?;
    #[cfg(unix)]
    {
        std::fs::rename(from, to).map_err(|e| format!("替换程序失败: {e}"))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        let backup = backup_path(to);
        let _ = std::fs::remove_file(&backup);
        std::fs::rename(to, &backup).map_err(|e| format!("移开正在运行的程序失败: {e}"))?;
        if let Err(e) = std::fs::rename(from, to) {
            let _ = std::fs::rename(&backup, to);
            return Err(format!("写入新程序失败: {e}"));
        }
        let _ = std::fs::remove_file(&backup);
        Ok(())
    }
}

/// Replacer child entry: wait for the parent to exit, swap, restart, quit.
pub fn apply_replacement(
    from: &Path,
    to: &Path,
    wait_for_parent: bool,
    restart_args: &[String],
) -> Result<(), String> {
    if !wait_for_parent || from.as_os_str().is_empty() || to.as_os_str().is_empty() {
        return Err("替换参数不完整".into());
    }
    let current = std::env::current_exe().map_err(|e| format!("找不到当前程序: {e}"))?;
    let current = current
        .canonicalize()
        .map_err(|e| format!("无法解析当前程序路径: {e}"))?;
    let to = to
        .canonicalize()
        .map_err(|e| format!("无法解析替换目标: {e}"))?;
    if current != to {
        return Err("只能替换当前正在运行的程序".into());
    }
    let from = from
        .canonicalize()
        .map_err(|e| format!("无法解析新程序: {e}"))?;
    validate_swap_paths(&from, &to)?;
    wait_for_parent_exit()?;
    let mut last = "替换程序失败".to_string();
    for _ in 0..50 {
        match swap_executable(&from, &to) {
            Ok(()) => {
                restart_process(&to, restart_args)?;
                std::process::exit(0);
            }
            Err(e) => {
                last = e;
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
    Err(last)
}

/// Spawn the replacer child and exit so it can take over the exe file.
pub fn relaunch_and_exit(staged: &Path, restart_args: &[String]) -> Result<(), String> {
    let exe = std::env::current_exe().map_err(|e| format!("找不到当前程序: {e}"))?;
    let mut command = std::process::Command::new(&exe);
    command
        .arg("--replace-from")
        .arg(staged)
        .arg("--replace-to")
        .arg(&exe)
        .arg("--wait-for-parent");
    for arg in restart_args {
        command.arg("--restart-arg").arg(arg);
    }
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    match command.spawn() {
        Ok(child) => {
            // Hold the stdin write end: the child treats EOF as "parent gone".
            std::mem::forget(child);
            eprintln!("正在退出以便完成程序更新");
            std::process::exit(0);
        }
        Err(e) => Err(format!("无法启动替换进程: {e}")),
    }
}

pub fn parse_sha256_file(text: &str) -> Result<String, String> {
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let token = line.split_whitespace().next().unwrap_or("");
        if token.len() == 64 && token.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(token.to_ascii_lowercase());
        }
    }
    Err("校验文件里没有 SHA256".into())
}

/// Asset host allowlist: official GitHub feed -> GitHub hosts only;
/// custom feed -> same host, or loopback for local testing.
pub fn ensure_asset_url(feed: &str, asset: &str) -> Result<(), String> {
    let asset_url = reqwest::Url::parse(asset).map_err(|e| format!("更新包地址无效: {e}"))?;
    let feed_url = reqwest::Url::parse(feed).map_err(|e| format!("程序更新源无效: {e}"))?;
    if official_source(feed) {
        let host = asset_url.host_str().unwrap_or("");
        let allowed = asset_url.scheme() == "https"
            && (host == "github.com"
                || host.ends_with(".github.com")
                || host.ends_with(".githubusercontent.com"));
        if !allowed {
            return Err(format!("程序更新包只能从 GitHub 下载，当前是 {asset}"));
        }
        return Ok(());
    }
    let same_host = asset_url.host_str() == feed_url.host_str();
    let loopback = host_is_loopback(asset_url.host_str());
    let scheme_ok = asset_url.scheme() == "https" || (asset_url.scheme() == "http" && loopback);
    if scheme_ok && (same_host || loopback && host_is_loopback(feed_url.host_str())) {
        Ok(())
    } else {
        Err(format!("程序更新包地址不在更新源允许的范围内: {asset}"))
    }
}

pub fn validate_feed(feed: &str) -> Result<(), String> {
    let feed = feed.trim();
    if feed.is_empty() {
        return Ok(());
    }
    let url = reqwest::Url::parse(feed).map_err(|e| format!("bad self_update_feed: {e}"))?;
    if url.scheme() == "https" || (url.scheme() == "http" && host_is_loopback(url.host_str())) {
        Ok(())
    } else {
        Err("self_update_feed must be https (or http on loopback)".to_string())
    }
}

/// Official flows (REST API or static manifest) may only pull program
/// archives from GitHub hosts; anything else falls under custom-feed rules.
fn official_source(feed: &str) -> bool {
    if github_repo(feed).is_some() {
        return true;
    }
    if let Ok(url) = reqwest::Url::parse(feed) {
        return url.host_str() == Some("github.com")
            && url
                .path()
                .starts_with("/ModerRAS/maa-fleet-updater/releases/");
    }
    false
}

fn github_repo(feed: &str) -> Option<(String, String)> {
    let url = reqwest::Url::parse(feed).ok()?;
    if url.host_str()? != "api.github.com" {
        return None;
    }
    let mut seg = url.path_segments()?;
    if seg.next()? != "repos" {
        return None;
    }
    let owner = seg.next()?.to_string();
    let repo = seg.next()?.to_string();
    match (seg.next(), seg.next()) {
        (Some("releases"), Some("latest")) => Some((owner, repo)),
        _ => None,
    }
}

fn host_is_loopback(host: Option<&str>) -> bool {
    matches!(host, Some("localhost") | Some("127.0.0.1") | Some("::1"))
}

fn safe_file_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains("..")
        || name == ASSET_ZIP
    {
        // Only our own known asset name is accepted.
        if name != ASSET_ZIP {
            return Err(format!("更新包文件名不在预期内: {name}"));
        }
    }
    Ok(name.to_string())
}

fn staged_path(exe: &Path) -> PathBuf {
    let name = exe.file_name().unwrap_or_default();
    let mut staged = name.to_os_string();
    staged.push(".new");
    exe.with_file_name(staged)
}

#[cfg(windows)]
fn backup_path(exe: &Path) -> PathBuf {
    let name = exe.file_name().unwrap_or_default();
    let mut backup = name.to_os_string();
    backup.push(".old");
    exe.with_file_name(backup)
}

fn validate_swap_paths(from: &Path, to: &Path) -> Result<(), String> {
    let from_name = from.file_name().ok_or("替换来源缺少文件名")?;
    let to_name = to.file_name().ok_or("替换目标缺少文件名")?;
    let mut expected = to_name.to_os_string();
    expected.push(".new");
    if from_name != expected.as_os_str() {
        return Err("替换来源必须是目标旁边的 .new 文件".into());
    }
    if from.parent() != to.parent() {
        return Err("替换文件必须和当前程序在同一目录".into());
    }
    if !from.is_file() {
        return Err("找不到新程序文件".into());
    }
    Ok(())
}

fn restart_process(exe: &Path, args: &[String]) -> Result<(), String> {
    std::process::Command::new(exe)
        .arg("--from-self-update")
        .args(args)
        .stdin(std::process::Stdio::null())
        .spawn()
        .map(|_| ())
        .map_err(|e| format!("重新启动失败: {e}"))
}

/// Parent keeps the stdin pipe open; EOF (or timeout) releases the child.
fn wait_for_parent_exit() -> Result<(), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin();
        let mut buf = [0u8; 32];
        let result = std::io::Read::read(&mut stdin, &mut buf);
        let _ = tx.send(result);
    });
    match rx.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("等待原进程退出失败: {e}")),
        Err(_) => Err("等待原进程退出超时".into()),
    }
}

#[derive(Debug, serde::Deserialize)]
struct ReleaseDocument {
    tag_name: String,
    #[serde(default)]
    target_commitish: Option<String>,
    #[serde(default)]
    commit: Option<String>,
    #[serde(default)]
    assets: Vec<ReleaseAsset>,
}

#[derive(Debug, serde::Deserialize)]
struct ReleaseAsset {
    name: String,
    browser_download_url: String,
    size: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
struct GitRef {
    object: GitObject,
}

#[derive(Debug, serde::Deserialize)]
struct GitObject {
    #[serde(rename = "type")]
    kind: String,
    sha: String,
}

#[derive(Debug, serde::Deserialize)]
struct GitTag {
    object: GitObject,
}

async fn fetch_tag_commit(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    tag: &str,
) -> Result<String, String> {
    if !tag.starts_with('v') || parse_semver(tag.trim_start_matches('v')).is_err() {
        return Err("标签不是稳定版本".into());
    }
    let url = format!("https://api.github.com/repos/{owner}/{repo}/git/refs/tags/{tag}");
    let text = get_text(client, &url, &url, MAX_TEXT_BYTES).await?;
    let reference: GitRef =
        serde_json::from_str(&text).map_err(|e| format!("标签信息无法解析: {e}"))?;
    if reference.object.kind == "commit" && looks_like_commit(&reference.object.sha) {
        return Ok(reference.object.sha);
    }
    if reference.object.kind == "tag" {
        let tag_url = format!(
            "https://api.github.com/repos/{owner}/{repo}/git/tags/{}",
            reference.object.sha
        );
        let text = get_text(client, &tag_url, &tag_url, MAX_TEXT_BYTES).await?;
        let annotated: GitTag =
            serde_json::from_str(&text).map_err(|e| format!("附注标签无法解析: {e}"))?;
        if annotated.object.kind == "commit" && looks_like_commit(&annotated.object.sha) {
            return Ok(annotated.object.sha);
        }
    }
    Err("标签没有指向提交".into())
}

async fn get_text(
    client: &reqwest::Client,
    url: &str,
    feed: &str,
    max_bytes: u64,
) -> Result<String, String> {
    let bytes = get_bytes(client, url, feed, max_bytes).await?;
    String::from_utf8(bytes).map_err(|_| "响应不是 UTF-8".to_string())
}

async fn get_bytes(
    client: &reqwest::Client,
    url: &str,
    feed: &str,
    max_bytes: u64,
) -> Result<Vec<u8>, String> {
    let mut next = url.to_string();
    for _ in 0..6 {
        ensure_asset_url(feed, &next)?;
        let resp = client
            .get(&next)
            .header("Accept", "application/vnd.github+json")
            .header("User-Agent", "maa-fleet-updater")
            .send()
            .await
            .map_err(|e| format!("请求更新信息失败: {e}"))?;
        if resp.status().is_redirection() {
            let loc = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or_else(|| "重定向缺少 Location".to_string())?;
            let loc = loc.to_str().map_err(|_| "重定向地址无效".to_string())?;
            let base = reqwest::Url::parse(&next).map_err(|e| format!("更新地址无效: {e}"))?;
            next = base
                .join(loc)
                .map_err(|_| "重定向地址无效".to_string())?
                .to_string();
            continue;
        }
        let final_url = resp.url().clone();
        ensure_asset_url(feed, final_url.as_str())?;
        if !resp.status().is_success() {
            return Err(http_error_with_body(resp).await);
        }
        return read_capped(resp, max_bytes).await;
    }
    Err("重定向过多".into())
}

/// Non-2xx updater response: keep the status AND a body snippet (GitHub
/// explains rate limits in the body), so the log actually says why.
async fn http_error_with_body(resp: reqwest::Response) -> String {
    let status = resp.status();
    let snippet = read_capped(resp, 2048).await.unwrap_or_default();
    let mut msg = format!("更新请求返回 HTTP {status}");
    let text = String::from_utf8_lossy(&snippet);
    let short: String = text.trim().chars().take(300).collect();
    if !short.is_empty() {
        msg.push_str(&format!(": {short}"));
    }
    if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        msg.push_str("（可能是 GitHub API 限流，稍后再试；或配置代理）");
    }
    msg
}

async fn read_capped(resp: reqwest::Response, max_bytes: u64) -> Result<Vec<u8>, String> {
    if let Some(len) = resp.content_length() {
        if len > max_bytes {
            return Err("更新响应过大".into());
        }
    }
    let mut body = Vec::new();
    let mut stream = resp.bytes_stream();
    use futures_util::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("读取更新响应失败: {e}"))?;
        if body.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err("更新响应过大".into());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn download_file(
    client: &reqwest::Client,
    url: &str,
    feed: &str,
    dest_part: &Path,
    max_bytes: u64,
) -> Result<(), String> {
    if let Some(parent) = dest_part.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("创建目录失败: {e}"))?;
        }
    }
    let mut next = url.to_string();
    for _ in 0..6 {
        ensure_asset_url(feed, &next)?;
        let resp = client
            .get(&next)
            .header("User-Agent", "maa-fleet-updater")
            .send()
            .await
            .map_err(|e| format!("下载失败: {e}"))?;
        if resp.status().is_redirection() {
            let loc = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .ok_or_else(|| "重定向缺少 Location".to_string())?;
            let loc = loc.to_str().map_err(|_| "重定向地址无效".to_string())?;
            let base = reqwest::Url::parse(&next).map_err(|e| format!("更新地址无效: {e}"))?;
            next = base
                .join(loc)
                .map_err(|_| "重定向地址无效".to_string())?
                .to_string();
            continue;
        }
        let final_url = resp.url().clone();
        ensure_asset_url(feed, final_url.as_str())?;
        if !resp.status().is_success() {
            return Err(http_error_with_body(resp).await);
        }
        if let Some(len) = resp.content_length() {
            if len > max_bytes {
                return Err("程序包过大".into());
            }
        }
        let tmp = dest_part.to_path_buf();
        let mut f = tokio::fs::File::create(&tmp)
            .await
            .map_err(|e| format!("创建文件失败: {e}"))?;
        let mut done: u64 = 0;
        let mut stream = resp.bytes_stream();
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("下载中断: {e}"))?;
            done = done.saturating_add(chunk.len() as u64);
            if done > max_bytes {
                drop(f);
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err("程序包过大".into());
            }
            f.write_all(&chunk)
                .await
                .map_err(|e| format!("写入失败: {e}"))?;
        }
        f.flush().await.map_err(|e| format!("保存失败: {e}"))?;
        return Ok(());
    }
    Err("重定向过多".into())
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = File::open(path).map_err(|e| format!("读取文件失败: {e}"))?;
    let mut hasher = sha2::Sha256::new();
    std::io::copy(&mut file, &mut hasher).map_err(|e| format!("校验失败: {e}"))?;
    use sha2::Digest;
    Ok(format!("{:x}", hasher.finalize()))
}

/// Only the daemon exe itself is extracted; everything else is ignored.
/// Zip-slip (.. / absolute paths) is rejected by enclosed_name().
fn extract_binary(archive: &Path, dest: &Path) -> Result<(), String> {
    let file = File::open(archive).map_err(|e| format!("打开压缩包失败: {e}"))?;
    let mut zip = zip::ZipArchive::new(file).map_err(|e| format!("压缩包无法读取: {e}"))?;
    if zip.len() > MAX_ENTRIES {
        return Err("压缩包条目过多".into());
    }
    for i in 0..zip.len() {
        let mut entry = zip
            .by_index(i)
            .map_err(|e| format!("读取压缩条目失败: {e}"))?;
        if entry.is_dir() {
            continue;
        }
        let name = entry
            .enclosed_name()
            .ok_or_else(|| "压缩包包含非法路径".to_string())?;
        let file_name = name.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if !file_name.eq_ignore_ascii_case(EXE_NAME) {
            continue;
        }
        copy_limited(&mut entry, dest)?;
        verify_executable_magic(dest)?;
        return Ok(());
    }
    Err(format!("压缩包里没有 {EXE_NAME}"))
}

fn copy_limited(reader: &mut impl Read, dest: &Path) -> Result<(), String> {
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| format!("创建更新目录失败: {e}"))?;
        }
    }
    let mut out = File::create(dest).map_err(|e| format!("写出程序失败: {e}"))?;
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| format!("解压程序失败: {e}"))?;
        if n == 0 {
            break;
        }
        total = total.saturating_add(n as u64);
        if total > MAX_BINARY_BYTES {
            let _ = fs::remove_file(dest);
            return Err("解压后的程序超过 80MB".into());
        }
        out.write_all(&buf[..n])
            .map_err(|e| format!("写出程序失败: {e}"))?;
    }
    if total == 0 {
        let _ = fs::remove_file(dest);
        return Err("解压后的程序是空的".into());
    }
    out.sync_all().map_err(|e| format!("保存程序失败: {e}"))?;
    Ok(())
}

fn verify_executable_magic(path: &Path) -> Result<(), String> {
    let mut file = File::open(path).map_err(|e| format!("读取新程序失败: {e}"))?;
    let mut magic = [0u8; 2];
    file.read_exact(&mut magic)
        .map_err(|_| "解出的文件不是可执行程序".to_string())?;
    if magic == *b"MZ" {
        Ok(())
    } else {
        let _ = fs::remove_file(path);
        Err("解出的文件不是可执行程序".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn release_with(tag: &str, commit: Option<&str>) -> RemoteRelease {
        RemoteRelease {
            tag: tag.to_string(),
            version: tag.trim_start_matches('v').to_string(),
            commit: commit.map(|s| s.to_string()),
            assets: vec![
                RemoteAsset {
                    name: ASSET_ZIP.to_string(),
                    url: "https://github.com/x/y/releases/download/v0.2.0/maa-fleet-updater-win-x64.zip".to_string(),
                    size: None,
                },
                RemoteAsset {
                    name: format!("{ASSET_ZIP}.sha256"),
                    url: "https://github.com/x/y/releases/download/v0.2.0/maa-fleet-updater-win-x64.zip.sha256".to_string(),
                    size: None,
                },
            ],
        }
    }

    #[test]
    fn newer_remote_plans_download() {
        let r = release_with("v0.2.0", None);
        match decide_update("0.1.0", Some("abcdef1"), &r).unwrap() {
            UpdateDecision::Update(p) => assert_eq!(p.version, "0.2.0"),
            UpdateDecision::Keep { message } => panic!("should update: {message}"),
        }
    }

    #[test]
    fn older_remote_keeps() {
        let r = release_with("v0.1.0", None);
        match decide_update("0.2.0", Some("abcdef1"), &r).unwrap() {
            UpdateDecision::Keep { .. } => {}
            UpdateDecision::Update(_) => panic!("must not downgrade"),
        }
    }

    #[test]
    fn same_version_same_commit_keeps() {
        let r = release_with("v0.1.0", Some("abcdef1234"));
        match decide_update("0.1.0", Some("abcdef1234"), &r).unwrap() {
            UpdateDecision::Keep { .. } => {}
            UpdateDecision::Update(_) => panic!("same build must not re-download"),
        }
    }

    #[test]
    fn same_version_different_commit_updates() {
        let r = release_with("v0.1.0", Some("1234567890"));
        match decide_update("0.1.0", Some("abcdef1234"), &r).unwrap() {
            UpdateDecision::Update(p) => assert_eq!(p.version, "0.1.0"),
            UpdateDecision::Keep { message } => panic!("moved tag must update: {message}"),
        }
    }

    #[test]
    fn same_version_without_commit_info_keeps() {
        let r = release_with("v0.1.0", None);
        match decide_update("0.1.0", None, &r).unwrap() {
            UpdateDecision::Keep { .. } => {}
            UpdateDecision::Update(_) => panic!("no commit info must not loop"),
        }
    }

    #[test]
    fn missing_asset_is_error() {
        let r = RemoteRelease {
            tag: "v0.2.0".to_string(),
            version: "0.2.0".to_string(),
            commit: None,
            assets: vec![],
        };
        assert!(decide_update("0.1.0", None, &r).is_err());
    }

    #[test]
    fn sha256_file_parses() {
        let text = "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3da2b0d0f7a7c2  maa-fleet-updater-win-x64.zip\n";
        assert_eq!(parse_sha256_file(text).unwrap().len(), 64);
        assert!(parse_sha256_file("no hash here").is_err());
    }

    #[test]
    fn official_manifest_allows_github_hosts() {
        assert!(official_source(DEFAULT_FEED));
        assert!(official_source(MANIFEST_URL));
        assert!(!official_source("http://127.0.0.1:8080/x"));
        // CDN redirect target for release assets.
        assert!(ensure_asset_url(
            MANIFEST_URL,
            "https://objects.githubusercontent.com/abc/maa-fleet-updater-win-x64.zip"
        )
        .is_ok());
        assert!(ensure_asset_url(MANIFEST_URL, "https://evil.com/x.zip").is_err());
    }

    #[test]
    fn feed_rules() {
        assert!(validate_feed("").is_ok());
        assert!(validate_feed(DEFAULT_FEED).is_ok());
        assert!(validate_feed("http://127.0.0.1:8080/releases/latest").is_ok());
        assert!(validate_feed("http://example.com/x").is_err());
        assert!(ensure_asset_url(
            DEFAULT_FEED,
            "https://github.com/a/b/releases/download/v0.2.0/maa-fleet-updater-win-x64.zip"
        )
        .is_ok());
        assert!(ensure_asset_url(
            DEFAULT_FEED,
            "https://evil.com/maa-fleet-updater-win-x64.zip"
        )
        .is_err());
    }

    #[test]
    fn swap_paths_validated() {
        let dir = std::env::temp_dir();
        assert!(validate_swap_paths(&dir.join("a.new"), &dir.join("a.exe")).is_err());
        let exe = dir.join("maa-fleet-updater.exe");
        let new = dir.join("maa-fleet-updater.exe.new");
        // Missing file also fails (no .new lying around in temp).
        let _ = std::fs::remove_file(&new);
        assert!(validate_swap_paths(&new, &exe).is_err());
    }

    /// End-to-end staging against a local feed: JSON + zip + sha256.
    #[tokio::test]
    async fn stages_from_local_feed() {
        let mut root = std::env::temp_dir();
        root.push(format!(
            "maa-fleet-ut-su-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&root).unwrap();
        // Fake exe payload with MZ magic.
        let mut payload = b"MZ".to_vec();
        payload.extend_from_slice(&[0u8; 1024]);
        // Build the release zip the same way CI does (exe at top level).
        let zip_path = root.join(ASSET_ZIP);
        {
            let f = File::create(&zip_path).unwrap();
            let mut w = zip::ZipWriter::new(f);
            w.start_file(EXE_NAME, zip::write::SimpleFileOptions::default())
                .unwrap();
            w.write_all(&payload).unwrap();
            w.start_file("README.md", zip::write::SimpleFileOptions::default())
                .unwrap();
            w.write_all(b"docs").unwrap();
            w.finish().unwrap();
        }
        let mut hasher = sha2::Sha256::new();
        use sha2::Digest;
        let mut zf = File::open(&zip_path).unwrap();
        std::io::copy(&mut zf, &mut hasher).unwrap();
        let hex = format!("{:x}", hasher.finalize());
        let sha_text = format!("{hex}  {ASSET_ZIP}\n");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let feed = format!("http://127.0.0.1:{port}/releases/latest");
        let zip_bytes = std::fs::read(&zip_path).unwrap();
        let release_json = serde_json::json!({
            "tag_name": "v9.9.9",
            "target_commitish": "main",
            "assets": [
                {"name": ASSET_ZIP, "browser_download_url": format!("http://127.0.0.1:{port}/{ASSET_ZIP}"), "size": zip_bytes.len()},
                {"name": format!("{ASSET_ZIP}.sha256"), "browser_download_url": format!("http://127.0.0.1:{port}/{ASSET_ZIP}.sha256"), "size": sha_text.len()},
            ]
        })
        .to_string();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = vec![0u8; 4096];
                let Ok(n) = sock.read(&mut buf).await else {
                    break;
                };
                let req = String::from_utf8_lossy(&buf[..n]);
                let (status, ctype, body): (&str, &str, Vec<u8>) =
                    if req.contains("/releases/latest") {
                        (
                            "200 OK",
                            "application/json",
                            release_json.clone().into_bytes(),
                        )
                    } else if req.contains(".sha256") {
                        ("200 OK", "text/plain", sha_text.clone().into_bytes())
                    } else {
                        ("200 OK", "application/zip", zip_bytes.clone())
                    };
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(&body).await;
            }
        });
        let client = build_client("").unwrap();
        let work = root.join("work");
        let fake_exe = root.join(EXE_NAME);
        let req = StageRequest {
            client: &client,
            feed: &feed,
            work_dir: &work,
            install_exe: &fake_exe,
            events: None,
        };
        let out = stage_release(&req).await.expect("stage");
        match out {
            StageOutcome::UpToDate { message } => panic!("expected stage, got: {message}"),
            StageOutcome::Staged { path, version } => {
                assert_eq!(version, "9.9.9");
                assert_eq!(path, fake_exe.with_file_name(format!("{EXE_NAME}.new")));
                let got = std::fs::read(&path).unwrap();
                assert_eq!(got, payload);
            }
        }
        server.abort();
        let _ = std::fs::remove_dir_all(&root);
    }
}
