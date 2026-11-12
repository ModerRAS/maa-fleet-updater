use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    pub dir: PathBuf,
    pub exe_version_raw: String,
    pub version_norm: Option<String>,
    /// maaApi channel name: stable | beta | alpha (follows each instance's own setting)
    pub channel: String,
    pub resource_time: Option<String>,
    pub running: bool,
    pub pending_package: Option<String>,
    /// MAA.Updater.exe next to MAA.exe (delegation target for app updates).
    #[serde(default)]
    pub updater_present: bool,
    /// Content (trimmed) of pending-update-failure.txt written by the updater.
    #[serde(default)]
    pub update_failure: Option<String>,
    /// Leftover NewVersionExtract dir = delegation started but never cleaned.
    #[serde(default)]
    pub extract_leftover: bool,
    #[serde(default)]
    pub update_success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanItem {
    pub dir: PathBuf,
    pub cur_raw: String,
    pub latest: String,
    pub channel: String,
    /// "ota" | "full" | "none"
    pub kind: String,
    pub file_name: String,
    pub urls: Vec<String>,
    #[serde(default)]
    pub downloaded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Plan {
    #[serde(default)]
    pub items: Vec<PlanItem>,
    #[serde(default)]
    pub resource_time: Option<String>,
}

pub fn event_bus() -> (broadcast::Sender<String>, broadcast::Receiver<String>) {
    broadcast::channel(512)
}

pub fn emit(tx: &broadcast::Sender<String>, level: &str, msg: impl Into<String>) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let line = serde_json::json!({"ts": ts, "level": level, "msg": msg.into()}).to_string();
    let _ = tx.send(line.clone());
    push_log(&line);
    if level == "error" {
        tracing::error!("{}", line);
    } else {
        tracing::info!("{}", line);
    }
}

/// ---- Retrievable logs: in-memory ring + on-disk file ----
/// The SSE /events stream is live-only; this store lets the WebUI pull
/// history after the fact and survives daemon restarts via staging/logs/.
const LOG_CAP: usize = 3000;
const LOG_FILE_MAX: u64 = 5 * 1024 * 1024;

struct LogStore {
    lines: std::sync::Mutex<VecDeque<String>>,
    dir: std::sync::Mutex<Option<PathBuf>>,
}

static LOG_STORE: std::sync::OnceLock<LogStore> = std::sync::OnceLock::new();

fn store() -> &'static LogStore {
    LOG_STORE.get_or_init(|| LogStore {
        lines: std::sync::Mutex::new(VecDeque::new()),
        dir: std::sync::Mutex::new(None),
    })
}

/// Point the file sink at <staging>/logs (creates it). Call after config
/// load and whenever staging_dir changes via the WebUI.
pub fn init_log_store(staging: PathBuf) {
    let dir = staging.join("logs");
    let _ = std::fs::create_dir_all(&dir);
    *store().dir.lock().unwrap() = Some(dir);
}

/// Current log file, if the store has been initialized.
pub fn log_file_path() -> Option<PathBuf> {
    store()
        .dir
        .lock()
        .unwrap()
        .clone()
        .map(|d| d.join("fleet.log"))
}

fn push_log(line: &str) {
    {
        let mut g = store().lines.lock().unwrap();
        if g.len() >= LOG_CAP {
            g.pop_front();
        }
        g.push_back(line.to_string());
    }
    append_file(line);
}

fn append_file(line: &str) {
    let path = match log_file_path() {
        Some(p) => p,
        None => return,
    };
    if std::fs::metadata(&path)
        .map(|m| m.len() > LOG_FILE_MAX)
        .unwrap_or(false)
    {
        let old = path.with_extension("old.log");
        let _ = std::fs::remove_file(&old);
        let _ = std::fs::rename(&path, &old);
    }
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// Last `tail` lines (tail == 0 means all), optionally filtered by level.
/// Each line is the JSON envelope emit() produced.
pub fn read_logs(tail: usize, level: Option<&str>) -> Vec<String> {
    let g = store().lines.lock().unwrap();
    let mut v: Vec<String> = match level.filter(|s| !s.is_empty()) {
        Some(lv) => {
            let pat = format!("\"level\":\"{lv}\"");
            g.iter().filter(|s| s.contains(&pat)).cloned().collect()
        }
        None => g.iter().cloned().collect(),
    };
    if tail > 0 && v.len() > tail {
        v.drain(..v.len() - tail);
    }
    v
}
