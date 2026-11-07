use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

fn default_search_roots() -> Vec<PathBuf> {
    vec![PathBuf::from("D:/MAA")]
}
fn default_max_depth() -> usize {
    4
}
fn default_staging() -> PathBuf {
    PathBuf::from("./staging")
}
fn default_resource_repo() -> String {
    "https://github.com/MaaAssistantArknights/MaaResource".to_string()
}
fn default_branch() -> String {
    "main".to_string()
}
fn default_api_bases() -> Vec<String> {
    vec![
        "https://api.maa.plus/MaaAssistantArknights/api/".to_string(),
        "https://api2.maa.plus/MaaAssistantArknights/api/".to_string(),
    ]
}
fn default_true() -> bool {
    true
}
fn default_bind() -> String {
    "127.0.0.1:38080".to_string()
}
fn default_stagger() -> u64 {
    5
}
fn default_prepare_hours() -> u64 {
    6
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default = "default_search_roots")]
    pub search_roots: Vec<PathBuf>,
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
    #[serde(default = "default_staging")]
    pub staging_dir: PathBuf,
    #[serde(default = "default_resource_repo")]
    pub resource_repo: String,
    #[serde(default = "default_branch")]
    pub resource_branch: String,
    #[serde(default = "default_api_bases")]
    pub maa_api_bases: Vec<String>,
    /// e.g. "http://127.0.0.1:7890" or "socks5://127.0.0.1:1080". Empty = direct.
    #[serde(default)]
    pub proxy: String,
    #[serde(default = "default_true")]
    pub commit_on_startup: bool,
    #[serde(default = "default_true")]
    pub takeover_autostart: bool,
    #[serde(default = "default_bind")]
    pub web_bind: String,
    #[serde(default)]
    pub auth_token: String,
    #[serde(default = "default_stagger")]
    pub launch_stagger_secs: u64,
    /// Background prepare interval in hours (check+download+git pull). 0 = off.
    #[serde(default = "default_prepare_hours")]
    pub auto_prepare_hours: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read config {}: {e}", path.display()))?;
        let mut cfg: Config =
            serde_json::from_str(&text).map_err(|e| format!("parse config: {e}"))?;
        if cfg.staging_dir.is_relative() {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    cfg.staging_dir = parent.join(&cfg.staging_dir);
                }
            }
        }
        Ok(cfg)
    }
}
