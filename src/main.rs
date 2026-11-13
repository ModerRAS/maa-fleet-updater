mod autostart;
mod config;
mod discover;
mod downloader;
mod fsutil;
mod maa_api;
mod pe_version;
mod resource;
mod self_update;
mod stage;
mod state;

use axum::{
    body::Body,
    extract::{Query, State},
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use state::{emit, InstanceInfo, Plan, PlanItem};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone)]
struct AppState {
    inner: Arc<Inner>,
}

struct Inner {
    cfg_path: PathBuf,
    cfg: RwLock<config::Config>,
    /// Serializes prepare / commit / self-update replace: only one of them
    /// touches instances or this exe at a time.
    prepare_lock: tokio::sync::Mutex<()>,
    su: RwLock<self_update::SelfUpdateState>,
    commit_once: bool,
    tx: broadcast::Sender<String>,
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}
async fn health() -> &'static str {
    "ok"
}

async fn instances(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let list = tokio::task::spawn_blocking(move || discover::scan(&cfg))
        .await
        .unwrap_or_default();
    Json(list)
}

async fn plan(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let p = load_plan(&cfg);
    Json(p)
}

/// One call powering the dashboard: live scan + plan + last run summaries.
async fn status(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let cfg_path = state.inner.cfg_path.clone();
    let staging = cfg.staging_dir.clone();
    let c2 = cfg.clone();
    let (instances, auto) =
        tokio::task::spawn_blocking(move || (discover::scan(&c2), autostart::status()))
            .await
            .unwrap_or_default();
    let mut plan = load_plan(&cfg);
    // Drop entries left by older scans (removed instances, newly excluded
    // backup dirs): commit matches by dir anyway, this is display hygiene.
    plan.items
        .retain(|it| instances.iter().any(|i| i.dir == it.dir));
    Json(serde_json::json!({
        "instances": instances,
        "plan": plan,
        "last_prepare": read_json_opt(staging.join("last_prepare.json")),
        "last_commit": read_json_opt(staging.join("last_commit.json")),
        "autostart_registered": auto.0,
        "autostart_detail": auto.1,
        "config_path": cfg_path.to_string_lossy(),
        "self_update": state.inner.su.read().await.clone(),
        "bind_note": bind_note(&cfg),
        "log_file": crate::state::log_file_path(),
    }))
}

async fn get_config(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    Json(cfg)
}

async fn save_config(
    State(state): State<AppState>,
    Json(mut new_cfg): Json<config::Config>,
) -> impl IntoResponse {
    if let Err(e) = new_cfg.validate() {
        return (StatusCode::BAD_REQUEST, e).into_response();
    }
    let path = state.inner.cfg_path.clone();
    if let Err(e) = new_cfg.save_to(&path) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e).into_response();
    }
    let old = state.inner.cfg.read().await.clone();
    let restart = old.web_bind != new_cfg.web_bind;
    let staging_changed = old.staging_dir != new_cfg.staging_dir;
    *state.inner.cfg.write().await = new_cfg.clone();
    if staging_changed {
        state::init_log_store(new_cfg.staging_dir.clone());
    }
    Json(serde_json::json!({"saved": true, "restart_needed": restart})).into_response()
}

async fn preflight(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let r = tokio::task::spawn_blocking(move || {
        let git = match std::process::Command::new("git").arg("--version").output() {
            Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
            Ok(o) => format!("FAILED: {}", String::from_utf8_lossy(&o.stderr).trim()),
            Err(e) => format!("NOT FOUND: {e}"),
        };
        let (staging_ok, staging_msg) = match std::fs::create_dir_all(&cfg.staging_dir) {
            Err(e) => (false, format!("mkdir {}: {e}", cfg.staging_dir.display())),
            Ok(()) => {
                let probe = cfg.staging_dir.join(".write-test");
                match std::fs::write(&probe, b"ok") {
                    Err(e) => (false, format!("write {}: {e}", probe.display())),
                    Ok(()) => {
                        let _ = std::fs::remove_file(&probe);
                        (true, format!("writable: {}", cfg.staging_dir.display()))
                    }
                }
            }
        };
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let mut ds = Vec::new();
        for d in disks.iter() {
            ds.push(serde_json::json!({
                "mount": d.mount_point().to_string_lossy(),
                "total_gb": d.total_space() as f64 / 1e9,
                "avail_gb": d.available_space() as f64 / 1e9,
            }));
        }
        let exe = std::env::current_exe()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        serde_json::json!({
            "exe": exe,
            "git": git,
            "staging": cfg.staging_dir.to_string_lossy(),
            "staging_writable": staging_ok,
            "staging_msg": staging_msg,
            "disks": ds,
        })
    })
    .await
    .unwrap_or(serde_json::json!({"error": "preflight task failed"}));
    Json(r)
}

async fn autostart_status() -> impl IntoResponse {
    let (registered, detail) = tokio::task::spawn_blocking(autostart::status)
        .await
        .unwrap_or((false, String::new()));
    Json(
        serde_json::json!({"registered": registered, "detail": detail, "task": autostart::TASK_NAME}),
    )
}

#[derive(serde::Deserialize)]
struct AutostartBody {
    action: String,
}

async fn autostart_action(
    State(state): State<AppState>,
    Json(body): Json<AutostartBody>,
) -> impl IntoResponse {
    let cfg_path = state.inner.cfg_path.clone();
    let out = tokio::task::spawn_blocking(move || match body.action.as_str() {
        "enable" => {
            let exe = std::env::current_exe().map_err(|e| format!("current exe: {e}"))?;
            autostart::enable(&exe, &cfg_path)
        }
        "disable" => autostart::disable(),
        other => Err(format!("unknown action: {other}")),
    })
    .await;
    match out {
        Ok(Ok(msg)) => Json(serde_json::json!({"ok": true, "msg": msg})).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("task failed: {e}"),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize, Default)]
struct PrepareBody {
    #[serde(default)]
    force_full: bool,
}

async fn prepare(
    State(state): State<AppState>,
    Json(body): Json<PrepareBody>,
) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        run_prepare(&s, body.force_full).await;
    });
    Json(serde_json::json!({"started": true}))
}

async fn commit(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        run_commit_launch(&s).await;
    });
    Json(serde_json::json!({"started": true}))
}

async fn launch(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        let cfg = s.inner.cfg.read().await.clone();
        let tx = s.inner.tx.clone();
        let c2 = cfg.clone();
        let dirs = tokio::task::spawn_blocking(move || {
            discover::scan(&c2)
                .into_iter()
                .map(|i| i.dir)
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        tokio::task::spawn_blocking(move || {
            stage::launch_all(&cfg, &dirs, &tx);
        })
        .await
        .ok();
    });
    Json(serde_json::json!({"started": true}))
}

async fn self_update_check(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        run_self_update(&s, false).await;
    });
    Json(serde_json::json!({"started": true}))
}

async fn self_update_apply(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        run_self_update(&s, true).await;
    });
    Json(serde_json::json!({"started": true}))
}

async fn set_su(state: &AppState, decision: &str, note: &str, latest: Option<String>) {
    let mut su = state.inner.su.write().await;
    su.last_check_at = Some(now_ts());
    su.decision = decision.to_string();
    su.note = note.to_string();
    if latest.is_some() {
        su.latest = latest;
    }
}

/// Self-update check (+ optional apply). Yields while prepare/commit holds the
/// work lock, and never runs in --commit-once stager mode.
async fn run_self_update(state: &AppState, manual_apply: bool) {
    if state.inner.commit_once {
        return;
    }
    let tx = state.inner.tx.clone();
    let _guard = match state.inner.prepare_lock.try_lock() {
        Ok(g) => g,
        Err(_) => {
            emit(&tx, "warn", "self-update: 准备或提交正在进行，本轮跳过");
            return;
        }
    };
    let cfg = state.inner.cfg.read().await.clone();
    // Empty = official flow (static manifest, API fallback); set = that feed alone.
    let feed = cfg.self_update_feed.trim().to_string();
    let client = match self_update::build_client(cfg.proxy.trim()) {
        Ok(c) => c,
        Err(e) => {
            set_su(state, "error", &format!("http client: {e}"), None).await;
            emit(&tx, "error", format!("self-update: {e}"));
            return;
        }
    };
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            set_su(state, "error", &format!("current exe: {e}"), None).await;
            return;
        }
    };
    set_su(state, "checking", "正在查询最新发布 ...", None).await;
    let req = self_update::StageRequest {
        client: &client,
        feed: &feed,
        work_dir: &cfg.staging_dir.join("self-update"),
        install_exe: &exe,
        events: Some(tx.clone()),
    };
    match self_update::stage_release(&req).await {
        Err(e) => {
            set_su(state, "error", &e, None).await;
            emit(&tx, "error", format!("self-update 检查失败: {e}"));
        }
        Ok(self_update::StageOutcome::UpToDate { message }) => {
            set_su(
                state,
                "up_to_date",
                &message,
                Some(self_update::current_version().to_string()),
            )
            .await;
            emit(&tx, "info", format!("self-update: {message}"));
        }
        Ok(self_update::StageOutcome::Staged { path, version }) => {
            set_su(
                state,
                "staged",
                &format!("v{version} 已就绪，准备重启切换"),
                Some(version.clone()),
            )
            .await;
            let from_target = self_update::running_from_cargo_target();
            if manual_apply || (cfg.auto_apply_self_update && !from_target) {
                emit(&tx, "info", format!("self-update: 重启切换到 v{version}"));
                let args = self_update::restart_args_for(&state.inner.cfg_path);
                if let Err(e) = self_update::relaunch_and_exit(&path, &args) {
                    set_su(state, "error", &e, None).await;
                    emit(&tx, "error", format!("self-update 重启失败: {e}"));
                }
                // relaunch_and_exit exits the process on success.
            } else {
                emit(
                    &tx,
                    "info",
                    format!("self-update: v{version} 已暂存（自动应用已关或为开发构建），可在页面上手动应用"),
                );
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct InstanceLogQuery {
    dir: String,
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    tail: Option<usize>,
}

/// Diagnostic files we are willing to read inside an instance. Whitelisted
/// by exact relative path so this endpoint can never become an arbitrary
/// file reader, and the dir must match a discovered instance.
const INSTANCE_LOG_FILES: [&str; 4] = [
    "debug/pending-update-applier.log",
    "debug/maa.log",
    "pending-update-failure.txt",
    "pending-update-success.txt",
];

/// GET /api/instance-logs?dir=<instance>&file=debug/pending-update-applier.log&tail=200
/// MAA and its updater keep the only record of why an app update was
/// dropped; without this the failure is invisible over the network.
async fn instance_logs(
    State(state): State<AppState>,
    Query(q): Query<InstanceLogQuery>,
) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let want_file = q
        .file
        .clone()
        .unwrap_or_else(|| INSTANCE_LOG_FILES[0].to_string());
    if !INSTANCE_LOG_FILES.contains(&want_file.as_str()) {
        return (
            StatusCode::BAD_REQUEST,
            format!("file not allowed: {want_file}"),
        )
            .into_response();
    }
    let tail = q.tail.unwrap_or(200).clamp(1, 2000);
    let want_dir = q.dir.clone();
    let file_for_task = want_file.clone();
    let body = tokio::task::spawn_blocking(move || {
        let instances = discover::scan(&cfg);
        let target = instances.iter().find(|i| {
            let a = std::fs::canonicalize(&i.dir).unwrap_or_else(|_| i.dir.clone());
            let b = std::fs::canonicalize(&want_dir).unwrap_or_else(|_| PathBuf::from(&want_dir));
            a == b
        });
        let inst = match target {
            Some(i) => i,
            None => return Err("dir is not a discovered instance".to_string()),
        };
        let path = inst.dir.join(&file_for_task);
        let text = std::fs::read(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let text = String::from_utf8_lossy(&text);
        let mut lines: Vec<&str> = text.lines().collect();
        if lines.len() > tail {
            lines = lines.split_off(lines.len() - tail);
        }
        Ok::<String, String>(lines.join("\n"))
    })
    .await
    .unwrap_or_else(|e| Err(format!("task failed: {e}")));
    match body {
        Ok(text) => Json(serde_json::json!({"file": want_file, "text": text})).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e).into_response(),
    }
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    #[serde(default)]
    tail: Option<usize>,
    #[serde(default)]
    level: Option<String>,
}

/// Pulled history for the WebUI (the SSE stream is live-only).
/// GET /api/logs?tail=500&level=error -> {lines:[...], count}
async fn logs(Query(q): Query<LogsQuery>) -> impl IntoResponse {
    let tail = q.tail.unwrap_or(500).clamp(1, 3000);
    let level = q.level.clone();
    let lines =
        tokio::task::spawn_blocking(move || crate::state::read_logs(tail, level.as_deref()))
            .await
            .unwrap_or_default();
    Json(serde_json::json!({ "lines": lines, "count": lines.len() })).into_response()
}

/// Whole on-disk log as plain text (survives daemon restarts).
async fn logs_download() -> impl IntoResponse {
    let body = tokio::task::spawn_blocking(|| {
        crate::state::log_file_path()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .unwrap_or_default()
    })
    .await
    .unwrap_or_default();
    ([("content-type", "text/plain; charset=utf-8")], body).into_response()
}

async fn events(
    State(state): State<AppState>,
    Query(q): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let token = state.inner.cfg.read().await.auth_token.clone();
    if !token.is_empty() && q.get("token").map(|s| s.as_str()) != Some(token.as_str()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let rx = state.inner.tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| async move {
        r.ok()
            .map(|s| Ok::<Event, axum::Error>(Event::default().data(s)))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn auth(State(state): State<AppState>, req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_string();
    if path.starts_with("/api/") && path != "/api/health" {
        let token = state.inner.cfg.read().await.auth_token.clone();
        if !token.is_empty() {
            let ok =
                req.headers().get("x-token").and_then(|v| v.to_str().ok()) == Some(token.as_str());
            if !ok {
                return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
            }
        }
    }
    next.run(req).await
}

fn plan_path(cfg: &config::Config) -> PathBuf {
    cfg.staging_dir.join("plan.json")
}

fn load_plan(cfg: &config::Config) -> Plan {
    std::fs::read_to_string(plan_path(cfg))
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_plan(cfg: &config::Config, plan: &Plan) -> Result<(), String> {
    if cfg.staging_dir.as_os_str().is_empty() {
        return Err("staging_dir is empty; set it in WebUI config".to_string());
    }
    std::fs::create_dir_all(&cfg.staging_dir).map_err(|e| format!("mkdir staging: {e}"))?;
    let t = serde_json::to_string_pretty(plan).map_err(|e| format!("serialize plan: {e}"))?;
    std::fs::write(plan_path(cfg), t).map_err(|e| format!("write plan: {e}"))
}

/// Prepare phase (network allowed): check versions, download packages, git pull resource.
async fn run_prepare(state: &AppState, force_full: bool) {
    let tx = state.inner.tx.clone();
    let _guard = match state.inner.prepare_lock.try_lock() {
        Ok(g) => g,
        Err(_) => {
            emit(&tx, "warn", "prepare already running");
            return;
        }
    };
    let t0 = std::time::Instant::now();
    emit(&tx, "info", "prepare start (network phase)");
    let cfg = state.inner.cfg.read().await.clone();
    let instances = tokio::task::spawn_blocking({
        let c = cfg.clone();
        move || discover::scan(&c)
    })
    .await
    .unwrap_or_default();
    emit(
        &tx,
        "info",
        format!("discovered {} instance(s)", instances.len()),
    );
    let client = match downloader::build_client(&cfg.proxy) {
        Ok(c) => c,
        Err(e) => {
            emit(&tx, "error", format!("http client: {e}"));
            return;
        }
    };
    let arch_arm = std::env::consts::ARCH == "aarch64";
    // group by (channel, current version)
    let mut groups: HashMap<(String, String), Vec<InstanceInfo>> = HashMap::new();
    for i in &instances {
        if let Some(v) = i.version_norm.clone() {
            groups
                .entry((i.channel.clone(), v))
                .or_default()
                .push(i.clone());
        }
    }
    let mut plan = Plan::default();
    for ((channel, cur), members) in &groups {
        let cur_v = match pe_version::normalize(cur) {
            Some(v) => v,
            None => continue,
        };
        let cur_raw = members[0].exe_version_raw.clone();
        match maa_api::check(
            &client,
            &cfg.maa_api_bases,
            channel,
            &cur_raw,
            &cur_v,
            arch_arm,
        )
        .await
        {
            Err(e) => emit(&tx, "error", format!("check {channel} {cur}: {e}")),
            Ok(maa_api::CheckRes::UpToDate) => {
                emit(&tx, "info", format!("{channel} {cur}: already latest"));
                for m in members {
                    plan.items.push(PlanItem {
                        dir: m.dir.clone(),
                        cur_raw: cur_raw.clone(),
                        latest: cur.clone(),
                        channel: channel.clone(),
                        kind: "none".to_string(),
                        file_name: String::new(),
                        urls: vec![],
                        downloaded: true,
                    });
                }
            }
            Ok(maa_api::CheckRes::Available(ck)) => {
                let pick = if force_full {
                    ck.full.clone().or(ck.ota.clone())
                } else {
                    ck.ota.clone().or(ck.full.clone())
                };
                match pick {
                    None => {
                        emit(
                            &tx,
                            "warn",
                            format!("{} {}: no usable asset", channel, ck.latest_raw),
                        );
                        for m in members {
                            plan.items.push(PlanItem {
                                dir: m.dir.clone(),
                                cur_raw: cur_raw.clone(),
                                latest: ck.latest_raw.clone(),
                                channel: channel.clone(),
                                kind: "none".to_string(),
                                file_name: String::new(),
                                urls: vec![],
                                downloaded: true,
                            });
                        }
                    }
                    Some(a) => {
                        emit(
                            &tx,
                            "info",
                            format!(
                                "{} {} -> {} ({} {})",
                                channel, cur, ck.latest_raw, a.kind, a.file_name
                            ),
                        );
                        let dest = cfg.staging_dir.join("downloads").join(&a.file_name);
                        if force_full && dest.is_file() {
                            // Recovery path for a poisoned staging file: refetch
                            // from the network instead of trusting the old bytes.
                            let _ = std::fs::remove_file(&dest);
                            emit(&tx, "info", format!("{}: 强制重新下载", a.file_name));
                        }
                        let mut downloaded = dest.is_file();
                        if !downloaded {
                            let total_n = a.urls.len();
                            emit(
                                &tx,
                                "info",
                                format!("downloading {} ({} url(s))", a.file_name, total_n),
                            );
                            let txc = tx.clone();
                            let fname = a.file_name.clone();
                            match downloader::download_first(
                                &client,
                                &a.urls,
                                &dest,
                                move |done, total| {
                                    if let Some(t) = total {
                                        if t > 0 {
                                            let _ =
                                                txc.send(format!("progress {fname} {done}/{t}"));
                                        }
                                    }
                                },
                            )
                            .await
                            {
                                Ok(()) => {
                                    downloaded = true;
                                    emit(&tx, "info", format!("downloaded {}", a.file_name));
                                }
                                Err(e) => emit(
                                    &tx,
                                    "error",
                                    format!("download {} failed: {e}", a.file_name),
                                ),
                            }
                        } else {
                            emit(&tx, "info", format!("{} already staged", a.file_name));
                        }
                        for m in members {
                            plan.items.push(PlanItem {
                                dir: m.dir.clone(),
                                cur_raw: cur_raw.clone(),
                                latest: ck.latest_raw.clone(),
                                channel: channel.clone(),
                                kind: a.kind.clone(),
                                file_name: a.file_name.clone(),
                                urls: a.urls.clone(),
                                downloaded,
                            });
                        }
                    }
                }
            }
        }
    }
    for i in &instances {
        if i.version_norm.is_none() {
            emit(
                &tx,
                "warn",
                format!(
                    "{}: version unreadable, app update skipped",
                    i.dir.display()
                ),
            );
        }
    }
    // resource: central git clone update (network phase)
    let repo_dir = cfg.staging_dir.join("MaaResource");
    let mut resource_error: Option<String> = None;
    match resource::ensure_repo(
        &repo_dir,
        &cfg.resource_repo,
        &cfg.resource_branch,
        &cfg.proxy,
    )
    .await
    {
        Err(e) => {
            let m = format!("resource clone: {e}");
            emit(&tx, "error", m.clone());
            resource_error = Some(m);
        }
        Ok(()) => match resource::update_repo(&repo_dir, &cfg.resource_branch, &cfg.proxy).await {
            Err(e) => {
                let m = format!("resource pull: {e}");
                emit(&tx, "error", m.clone());
                resource_error = Some(m);
            }
            Ok(rev) => {
                let t = resource::read_last_updated(&resource::central_resource_dir(&repo_dir));
                plan.resource_time = t.clone();
                emit(&tx, "info", format!("resource updated to {rev} time={t:?}"));
            }
        },
    }
    match save_plan(&cfg, &plan) {
        Ok(()) => emit(&tx, "info", format!("prepare done in {:?}", t0.elapsed())),
        Err(e) => emit(&tx, "error", format!("save plan: {e}")),
    }
    let summary = serde_json::json!({
        "at": now_ts(),
        "items": plan.items.iter().map(|i| serde_json::json!({
            "dir": i.dir, "cur": i.cur_raw, "latest": i.latest,
            "kind": i.kind, "file": i.file_name, "downloaded": i.downloaded,
        })).collect::<Vec<_>>(),
        "resource_time": plan.resource_time,
        "resource_error": resource_error,
    });
    write_json(&cfg.staging_dir.join("last_prepare.json"), &summary);
}

/// Commit + launch (zero network): stage files into stopped instances, then launch all.
async fn run_commit_launch(state: &AppState) {
    let tx = state.inner.tx.clone();
    // Same work lock as prepare / self-update: never stage instances while a
    // self-update replace is in flight, and vice versa.
    let _work = state.inner.prepare_lock.lock().await;
    let cfg = state.inner.cfg.read().await.clone();
    let staging = cfg.staging_dir.clone();
    let plan = load_plan(&cfg);
    let reports = tokio::task::spawn_blocking({
        let c = cfg.clone();
        let t = tx.clone();
        move || stage::commit_all(&c, &plan, &t)
    })
    .await
    .unwrap_or_default();
    let dirs: Vec<PathBuf> = reports.iter().map(|r| r.dir.clone()).collect();
    let dirs = if dirs.is_empty() {
        let c = cfg.clone();
        tokio::task::spawn_blocking(move || {
            discover::scan(&c)
                .into_iter()
                .map(|i| i.dir)
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default()
    } else {
        dirs
    };
    let reps = tokio::task::spawn_blocking(move || stage::launch_all(&cfg, &dirs, &tx))
        .await
        .unwrap_or_default();
    for r in &reps {
        emit(
            &state.inner.tx,
            "info",
            format!("launch result {}: {}", r.dir.display(), r.note),
        );
    }
    let summary = serde_json::json!({ "at": now_ts(), "committed": reports, "launched": reps });
    write_json(&staging.join("last_commit.json"), &summary);
}

/// --logs: print recent on-disk log lines and exit (for shells and
/// agents, no WebUI needed). Reads fleet.log + fleet.old.log under
/// staging/logs. Exit 0 ok, 1 no logs yet, 2 bad config.
fn dump_logs(cfg_path: &Path, spec: &LogsSpec) -> i32 {
    let mut cfg = match config::Config::load(cfg_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("load config {}: {e}", cfg_path.display());
            return 2;
        }
    };
    cfg.resolve_staging(cfg_path);
    let dir = cfg.staging_dir.join("logs");
    let mut all: Vec<String> = Vec::new();
    for name in ["fleet.old.log", "fleet.log"] {
        if let Ok(t) = std::fs::read_to_string(dir.join(name)) {
            all.extend(t.lines().map(|s| s.to_string()));
        }
    }
    if all.is_empty() {
        eprintln!("no logs yet in {}", dir.display());
        return 1;
    }
    for line in select_logs(all, spec.tail, spec.level.as_deref()) {
        println!("{}", pretty_log(&line));
    }
    0
}

/// Shared tail+level selection (mirrors state::read_logs over the ring).
fn select_logs(all: Vec<String>, tail: usize, level: Option<&str>) -> Vec<String> {
    let v: Vec<String> = match level.filter(|s| !s.is_empty()) {
        Some(lv) => {
            let pat = format!("\"level\":\"{lv}\"");
            all.into_iter().filter(|s| s.contains(&pat)).collect()
        }
        None => all,
    };
    if tail > 0 && v.len() > tail {
        v[v.len() - tail..].to_vec()
    } else {
        v
    }
}

fn pretty_log(line: &str) -> String {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
        let ts = v["ts"].as_u64().map(|t| t.to_string()).unwrap_or_default();
        let lv = v["level"].as_str().unwrap_or("?");
        let msg = v["msg"].as_str().unwrap_or(line);
        return format!("[{ts}][{lv}] {msg}");
    }
    line.to_string()
}

fn now_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn write_json(path: &std::path::Path, v: &serde_json::Value) {
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if let Ok(t) = serde_json::to_string_pretty(v) {
        let _ = std::fs::write(path, t);
    }
}

fn read_json_opt(path: std::path::PathBuf) -> serde_json::Value {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn default_config_path() -> PathBuf {
    // Primary use is double-clicking the exe: keep config next to it,
    // independent of the process working directory.
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            if !dir.as_os_str().is_empty() {
                return dir.join("config.json");
            }
        }
    }
    PathBuf::from("config.json")
}

struct Args {
    cfg: PathBuf,
    once: bool,
    from_self_update: bool,
    logs: Option<LogsSpec>,
}

struct LogsSpec {
    tail: usize,
    level: Option<String>,
}

fn parse_args() -> Args {
    // No --config: primary double-click use, anchor next to the exe.
    // Explicit relative --config: classic CLI semantics, resolve against CWD.
    let mut cfg: Option<PathBuf> = None;
    let mut once = false;
    let mut from_self_update = false;
    let mut explicit = false;
    let mut want_logs = false;
    let mut logs_tail: usize = 200;
    let mut logs_level: Option<String> = None;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == "--config" {
            if let Some(v) = it.next() {
                cfg = Some(PathBuf::from(v));
                explicit = true;
            }
        } else if a == "--commit-once" {
            once = true;
        } else if a == "--from-self-update" {
            from_self_update = true;
        } else if a == "--logs" {
            want_logs = true;
        } else if a == "--logs-tail" {
            if let Some(v) = it.next() {
                logs_tail = v.parse().unwrap_or(200).clamp(1, 3000);
            }
        } else if a == "--logs-level" {
            if let Some(v) = it.next() {
                logs_level = Some(v);
            }
        }
    }
    let mut cfg = cfg.unwrap_or_else(default_config_path);
    let anchor = !explicit;
    // Default (double-click): anchor next to the exe.
    // Explicit --config: classic CLI semantics, resolve against CWD.
    if anchor {
        anchor_to_exe_dir(&mut cfg);
    }
    Args {
        cfg,
        once,
        from_self_update,
        logs: want_logs.then_some(LogsSpec {
            tail: logs_tail,
            level: logs_level,
        }),
    }
}

struct Replacer {
    from: PathBuf,
    to: PathBuf,
    wait: bool,
    restart_args: Vec<String>,
}

/// Hidden replacer-child args (only spawned by relaunch_and_exit).
/// Any presence routes the process into apply_replacement instead of the daemon.
fn parse_replacer() -> Option<Replacer> {
    let mut from: Option<PathBuf> = None;
    let mut to: Option<PathBuf> = None;
    let mut wait = false;
    let mut restart_args = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == "--replace-from" {
            from = it.next().map(PathBuf::from);
        } else if a == "--replace-to" {
            to = it.next().map(PathBuf::from);
        } else if a == "--wait-for-parent" {
            wait = true;
        } else if a == "--restart-arg" {
            if let Some(v) = it.next() {
                restart_args.push(v);
            }
        }
    }
    if from.is_some() || to.is_some() || wait {
        Some(Replacer {
            from: from.unwrap_or_default(),
            to: to.unwrap_or_default(),
            wait,
            restart_args,
        })
    } else {
        None
    }
}

/// Every relative path anchors to the executable directory, never CWD.
fn anchor_to_exe_dir(cfg: &mut PathBuf) {
    if cfg.is_relative() {
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                if !dir.as_os_str().is_empty() {
                    *cfg = dir.join(&*cfg);
                }
            }
        }
    }
}

/// First double-click experience: no config existed, so show the user where
/// to continue (WebUI in the default browser). Best effort only.
/// 0.0.0.0 / :: mean "LAN-visible" to the listener but are not openable
/// in a browser; hop through loopback for the first-run popup.
fn browser_url(bind: &str) -> String {
    match bind.parse::<std::net::SocketAddr>() {
        Ok(a) if a.ip().is_unspecified() => format!("http://127.0.0.1:{}", a.port()),
        _ => format!("http://{bind}"),
    }
}

/// WebUI-facing note: binding 0.0.0.0 without a token lets anyone on the
/// LAN operate the fleet (prepare/commit/launch). Empty = loopback, fine.
fn bind_note(cfg: &config::Config) -> String {
    let unspecified = cfg
        .web_bind
        .parse::<std::net::SocketAddr>()
        .map(|a| a.ip().is_unspecified())
        .unwrap_or(false);
    if !unspecified {
        return String::new();
    }
    if cfg.auth_token.trim().is_empty() {
        "警告：监听在 0.0.0.0 且未设 Token，局域网任何人可操作，建议设置 Token".to_string()
    } else {
        "监听在 0.0.0.0，局域网可访问（已设 Token）".to_string()
    }
}

fn open_browser_once(bind: &str) {
    let url = browser_url(bind);
    let _ = std::process::Command::new("cmd")
        .args(["/C", "start", "", &url])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

#[tokio::main]
async fn main() {
    // Replacer child (spawned by relaunch_and_exit): swap exe, restart, quit.
    // Runs before anything else; needs no config or async runtime.
    if let Some(rep) = parse_replacer() {
        if let Err(e) =
            self_update::apply_replacement(&rep.from, &rep.to, rep.wait, &rep.restart_args)
        {
            eprintln!("self-update replace failed: {e}");
            std::process::exit(1);
        }
        return;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args = parse_args();
    // Log dump for shells/agents: no daemon, no WebUI, stdout only.
    if let Some(spec) = &args.logs {
        std::process::exit(dump_logs(&args.cfg, spec));
    }
    let (cfg_path, commit_once, from_self_update) = (args.cfg, args.once, args.from_self_update);
    let cfg_path = std::fs::canonicalize(&cfg_path)
        .map(|p| crate::fsutil::from_verbatim(&p))
        .unwrap_or(cfg_path);
    let (cfg, cfg_note) = config::Config::load_or_create(&cfg_path);
    state::init_log_store(cfg.staging_dir.clone());
    let (tx, _rx) = state::event_bus();
    // Fresh defaults were just written (missing or corrupt config):
    // this is effectively a first run, open the WebUI for the user.
    let first_run = cfg_note.is_some();
    if let Some(note) = cfg_note {
        tracing::warn!("{note}");
    }
    tracing::info!(
        "config: {} staging: {}",
        cfg_path.display(),
        cfg.staging_dir.display()
    );
    if from_self_update {
        tracing::info!(
            "restarted by self-update: v{} ({})",
            self_update::current_version(),
            self_update::current_commit().unwrap_or("nocommit")
        );
    }
    let su_init = self_update::SelfUpdateState {
        current_version: self_update::current_version().to_string(),
        current_commit: self_update::current_commit().map(|s| s.to_string()),
        decision: "unknown".to_string(),
        note: "尚未检查".to_string(),
        ..Default::default()
    };
    let state = AppState {
        inner: Arc::new(Inner {
            cfg_path: cfg_path.clone(),
            cfg: RwLock::new(cfg),
            prepare_lock: tokio::sync::Mutex::new(()),
            su: RwLock::new(su_init),
            commit_once,
            tx,
        }),
    };
    {
        let c = state.inner.cfg.read().await.clone();
        state::emit(
            &state.inner.tx,
            "info",
            format!(
                "startup v{} ({}) config={} staging={} mode={}",
                self_update::current_version(),
                self_update::current_commit().unwrap_or("nocommit"),
                state.inner.cfg_path.display(),
                c.staging_dir.display(),
                c.resource_mode,
            ),
        );
    }
    if state.inner.cfg.read().await.commit_on_startup || commit_once {
        run_commit_launch(&state).await;
    }
    if commit_once {
        return;
    }
    // background auto prepare
    {
        let s = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            loop {
                let hours = s.inner.cfg.read().await.auto_prepare_hours;
                if hours > 0 {
                    run_prepare(&s, false).await;
                    tokio::time::sleep(std::time::Duration::from_secs(hours * 3600)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                }
            }
        });
    }
    // background self-update check (first round after boot settles)
    {
        let s = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            loop {
                let hours = s.inner.cfg.read().await.self_update_hours;
                if hours > 0 {
                    run_self_update(&s, false).await;
                    tokio::time::sleep(std::time::Duration::from_secs(hours * 3600)).await;
                } else {
                    tokio::time::sleep(std::time::Duration::from_secs(600)).await;
                }
            }
        });
    }
    let bind = state.inner.cfg.read().await.web_bind.clone();
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/instances", get(instances))
        .route("/api/plan", get(plan))
        .route("/api/status", get(status))
        .route("/api/preflight", get(preflight))
        .route("/api/prepare", post(prepare))
        .route("/api/commit", post(commit))
        .route("/api/launch", post(launch))
        .route("/api/config", get(get_config).post(save_config))
        .route(
            "/api/autostart",
            get(autostart_status).post(autostart_action),
        )
        .route("/api/self-update/check", post(self_update_check))
        .route("/api/self-update/apply", post(self_update_apply))
        .route("/api/logs", get(logs))
        .route("/api/instance-logs", get(instance_logs))
        .route("/api/logs/download", get(logs_download))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .unwrap_or_else(|e| {
            eprintln!("bind {bind}: {e}");
            std::process::exit(2);
        });
    tracing::info!("listening on {bind}");
    if first_run && !commit_once {
        open_browser_once(&bind);
    }
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn logs_tail_and_level_filter() {
        let all = vec![
            r#"{"ts":1,"level":"info","msg":"a"}"#.to_string(),
            r#"{"ts":2,"level":"error","msg":"b"}"#.to_string(),
            r#"{"ts":3,"level":"info","msg":"c"}"#.to_string(),
        ];
        assert_eq!(select_logs(all.clone(), 0, None).len(), 3);
        assert_eq!(select_logs(all.clone(), 2, None).len(), 2);
        let e = select_logs(all.clone(), 0, Some("error"));
        assert_eq!(e.len(), 1);
        assert!(e[0].contains('b'));
        assert_eq!(pretty_log(&all[1]), "[2][error] b");
        assert_eq!(pretty_log("raw"), "raw");
    }
}
