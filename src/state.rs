use serde::{Deserialize, Serialize};
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
    if level == "error" {
        tracing::error!("{}", line);
    } else {
        tracing::info!("{}", line);
    }
}
