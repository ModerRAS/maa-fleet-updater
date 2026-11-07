mod config;
mod discover;
mod downloader;
mod maa_api;
mod pe_version;
mod resource;
mod stage;
mod state;

use axum::{body::Body, extract::{Query, State}, http::{Request, StatusCode}, middleware::{self, Next}, response::{Html, IntoResponse, Response, sse::{Event, KeepAlive, Sse}}, routing::{get, post}, Json, Router};
use futures_util::StreamExt;
use state::{emit, InstanceInfo, Plan, PlanItem};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;

const INDEX_HTML: &str = include_str!("../web/index.html");

#[derive(Clone)]
struct AppState { inner: Arc<Inner> }

struct Inner {
    cfg_path: PathBuf,
    cfg: RwLock<config::Config>,
    prepare_lock: tokio::sync::Mutex<()>,
    tx: broadcast::Sender<String>,
}

async fn index() -> Html<&'static str> { Html(INDEX_HTML) }
async fn health() -> &'static str { "ok" }

async fn instances(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let list = tokio::task::spawn_blocking(move || discover::scan(&cfg)).await.unwrap_or_default();
    Json(list)
}

async fn plan(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.inner.cfg.read().await.clone();
    let p = load_plan(&cfg);
    Json(p)
}

#[derive(serde::Deserialize, Default)]
struct PrepareBody { #[serde(default)] force_full: bool }

async fn prepare(State(state): State<AppState>, Json(body): Json<PrepareBody>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move { run_prepare(&s, body.force_full).await; });
    Json(serde_json::json!({"started": true}))
}

async fn commit(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move { run_commit_launch(&s).await; });
    Json(serde_json::json!({"started": true}))
}

async fn launch(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.clone();
    tokio::spawn(async move {
        let cfg = s.inner.cfg.read().await.clone();
        let tx = s.inner.tx.clone();
        let c2 = cfg.clone();
        let dirs = tokio::task::spawn_blocking(move || {
            discover::scan(&c2).into_iter().map(|i| i.dir).collect::<Vec<_>>()
        }).await.unwrap_or_default();
        tokio::task::spawn_blocking(move || {
            stage::launch_all(&cfg, &dirs, &tx);
        }).await.ok();
    });
    Json(serde_json::json!({"started": true}))
}

async fn events(State(state): State<AppState>, Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let token = state.inner.cfg.read().await.auth_token.clone();
    if !token.is_empty() && q.get("token").map(|s| s.as_str()) != Some(token.as_str()) {
        return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
    }
    let rx = state.inner.tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|r| async move {
        r.ok().map(|s| Ok::<Event, axum::Error>(Event::default().data(s)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
}

async fn auth(State(state): State<AppState>, req: Request<Body>, next: Next) -> Response {
    let path = req.uri().path().to_string();
    if path.starts_with("/api/") && path != "/api/health" {
        let token = state.inner.cfg.read().await.auth_token.clone();
        if !token.is_empty() {
            let ok = req.headers().get("x-token").and_then(|v| v.to_str().ok()) == Some(token.as_str());
            if !ok { return (StatusCode::UNAUTHORIZED, "unauthorized").into_response(); }
        }
    }
    next.run(req).await
}

fn plan_path(cfg: &config::Config) -> PathBuf { cfg.staging_dir.join("plan.json") }

fn load_plan(cfg: &config::Config) -> Plan {
    std::fs::read_to_string(plan_path(cfg)).ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default()
}

fn save_plan(cfg: &config::Config, plan: &Plan) -> Result<(), String> {
    std::fs::create_dir_all(&cfg.staging_dir).map_err(|e| format!("mkdir staging: {e}"))?;
    let t = serde_json::to_string_pretty(plan).map_err(|e| format!("serialize plan: {e}"))?;
    std::fs::write(plan_path(cfg), t).map_err(|e| format!("write plan: {e}"))
}

/// Prepare phase (network allowed): check versions, download packages, git pull resource.
async fn run_prepare(state: &AppState, force_full: bool) {
    let tx = state.inner.tx.clone();
    let _guard = match state.inner.prepare_lock.try_lock() {
        Ok(g) => g,
        Err(_) => { emit(&tx, "warn", "prepare already running"); return; }
    };
    let t0 = std::time::Instant::now();
    emit(&tx, "info", "prepare start (network phase)");
    let cfg = state.inner.cfg.read().await.clone();
    let instances = tokio::task::spawn_blocking({ let c = cfg.clone(); move || discover::scan(&c) }).await.unwrap_or_default();
    emit(&tx, "info", format!("discovered {} instance(s)", instances.len()));
    let client = match downloader::build_client(&cfg.proxy) {
        Ok(c) => c,
        Err(e) => { emit(&tx, "error", format!("http client: {e}")); return; }
    };
    let arch_arm = std::env::consts::ARCH == "aarch64";
    // group by (channel, current version)
    let mut groups: HashMap<(String, String), Vec<InstanceInfo>> = HashMap::new();
    for i in &instances {
        if let Some(v) = i.version_norm.clone() {
            groups.entry((i.channel.clone(), v)).or_default().push(i.clone());
        }
    }
    let mut plan = Plan::default();
    for ((channel, cur), members) in &groups {
        let cur_v = match pe_version::normalize(cur) { Some(v) => v, None => continue };
        let cur_raw = members[0].exe_version_raw.clone();
        match maa_api::check(&client, &cfg.maa_api_bases, channel, &cur_raw, &cur_v, arch_arm).await {
            Err(e) => emit(&tx, "error", format!("check {channel} {cur}: {e}")),
            Ok(maa_api::CheckRes::UpToDate) => {
                emit(&tx, "info", format!("{channel} {cur}: already latest"));
                for m in members { plan.items.push(PlanItem { dir: m.dir.clone(), cur_raw: cur_raw.clone(), latest: cur.clone(), channel: channel.clone(), kind: "none".to_string(), file_name: String::new(), urls: vec![], downloaded: true }); }
            }
            Ok(maa_api::CheckRes::Available(ck)) => {
                let pick = if force_full { ck.full.clone().or(ck.ota.clone()) } else { ck.ota.clone().or(ck.full.clone()) };
                match pick {
                    None => {
                        emit(&tx, "warn", format!("{} {}: no usable asset", channel, ck.latest_raw));
                        for m in members { plan.items.push(PlanItem { dir: m.dir.clone(), cur_raw: cur_raw.clone(), latest: ck.latest_raw.clone(), channel: channel.clone(), kind: "none".to_string(), file_name: String::new(), urls: vec![], downloaded: true }); }
                    }
                    Some(a) => {
                        emit(&tx, "info", format!("{} {} -> {} ({} {})", channel, cur, ck.latest_raw, a.kind, a.file_name));
                        let dest = cfg.staging_dir.join("downloads").join(&a.file_name);
                        let mut downloaded = dest.is_file();
                        if !downloaded {
                            let total_n = a.urls.len();
                            emit(&tx, "info", format!("downloading {} ({} url(s))", a.file_name, total_n));
                            let txc = tx.clone();
                            let fname = a.file_name.clone();
                            match downloader::download_first(&client, &a.urls, &dest, move |done, total| {
                                if let Some(t) = total { if t > 0 { let _ = txc.send(format!("progress {fname} {done}/{t}")); } }
                            }).await {
                                Ok(()) => { downloaded = true; emit(&tx, "info", format!("downloaded {}", a.file_name)); }
                                Err(e) => emit(&tx, "error", format!("download {} failed: {e}", a.file_name)),
                            }
                        } else {
                            emit(&tx, "info", format!("{} already staged", a.file_name));
                        }
                        for m in members { plan.items.push(PlanItem { dir: m.dir.clone(), cur_raw: cur_raw.clone(), latest: ck.latest_raw.clone(), channel: channel.clone(), kind: a.kind.clone(), file_name: a.file_name.clone(), urls: a.urls.clone(), downloaded }); }
                    }
                }
            }
        }
    }
    for i in &instances {
        if i.version_norm.is_none() {
            emit(&tx, "warn", format!("{}: version unreadable, app update skipped", i.dir.display()));
        }
    }
    // resource: central git clone update (network phase)
    let repo_dir = cfg.staging_dir.join("MaaResource");
    match resource::ensure_repo(&repo_dir, &cfg.resource_repo, &cfg.resource_branch, &cfg.proxy).await {
        Err(e) => emit(&tx, "error", format!("resource clone: {e}")),
        Ok(()) => match resource::update_repo(&repo_dir, &cfg.resource_branch, &cfg.proxy).await {
            Err(e) => emit(&tx, "error", format!("resource pull: {e}")),
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
}

/// Commit + launch (zero network): stage files into stopped instances, then launch all.
async fn run_commit_launch(state: &AppState) {
    let tx = state.inner.tx.clone();
    let cfg = state.inner.cfg.read().await.clone();
    let plan = load_plan(&cfg);
    let reports = tokio::task::spawn_blocking({ let c = cfg.clone(); let t = tx.clone(); move || stage::commit_all(&c, &plan, &t) }).await.unwrap_or_default();
    let dirs: Vec<PathBuf> = reports.into_iter().map(|r| r.dir).collect();
    let dirs = if dirs.is_empty() {
        let c = cfg.clone();
        tokio::task::spawn_blocking(move || discover::scan(&c).into_iter().map(|i| i.dir).collect::<Vec<_>>()).await.unwrap_or_default()
    } else { dirs };
    tokio::task::spawn_blocking(move || { stage::launch_all(&cfg, &dirs, &tx); }).await.ok();
}

fn parse_args() -> (PathBuf, bool) {
    let mut cfg = PathBuf::from("config.json");
    let mut once = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == "--config" { if let Some(v) = it.next() { cfg = PathBuf::from(v); } }
        else if a == "--commit-once" { once = true; }
    }
    (cfg, once)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();
    let (cfg_path, commit_once) = parse_args();
    let cfg = config::Config::load(&cfg_path).unwrap_or_else(|e| {
        eprintln!("config error: {e} (see config.example.json)");
        std::process::exit(2);
    });
    let (tx, _rx) = state::event_bus();
    let state = AppState { inner: Arc::new(Inner { cfg_path, cfg: RwLock::new(cfg), prepare_lock: tokio::sync::Mutex::new(()), tx }) };
    if state.inner.cfg.read().await.commit_on_startup || commit_once {
        run_commit_launch(&state).await;
    }
    if commit_once { return; }
    // background auto prepare
    {
        let s = state.clone();
        tokio::spawn(async move {
            let hours = s.inner.cfg.read().await.auto_prepare_hours;
            if hours == 0 { return; }
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            loop {
                run_prepare(&s, false).await;
                tokio::time::sleep(std::time::Duration::from_secs(hours * 3600)).await;
            }
        });
    }
    let bind = state.inner.cfg.read().await.web_bind.clone();
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/instances", get(instances))
        .route("/api/plan", get(plan))
        .route("/api/prepare", post(prepare))
        .route("/api/commit", post(commit))
        .route("/api/launch", post(launch))
        .route("/events", get(events))
        .layer(middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind).await.unwrap_or_else(|e| {
        eprintln!("bind {bind}: {e}");
        std::process::exit(2);
    });
    tracing::info!("listening on {bind}");
    axum::serve(listener, app).await.unwrap();
}