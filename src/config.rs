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

impl Default for Config {
    fn default() -> Self {
        Config {
            search_roots: default_search_roots(),
            max_depth: default_max_depth(),
            staging_dir: default_staging(),
            resource_repo: default_resource_repo(),
            resource_branch: default_branch(),
            maa_api_bases: default_api_bases(),
            proxy: String::new(),
            commit_on_startup: true,
            takeover_autostart: true,
            web_bind: default_bind(),
            auth_token: String::new(),
            launch_stagger_secs: default_stagger(),
            auto_prepare_hours: default_prepare_hours(),
        }
    }
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

    /// Load, creating a default file when missing. A corrupt file is backed
    /// up as .bad.<timestamp> and replaced with defaults. Never fails closed:
    /// the daemon always starts; problems are reported, not fatal.
    pub fn load_or_create(path: &Path) -> (Self, Option<String>) {
        if !path.exists() {
            let cfg = Config::default();
            match cfg.save_to(path) {
                Ok(()) => {
                    return (
                        cfg,
                        Some(format!("created default config at {}", path.display())),
                    )
                }
                Err(e) => {
                    return (
                        cfg,
                        Some(format!(
                            "config missing and create failed ({e}), using in-memory defaults"
                        )),
                    )
                }
            }
        }
        match Self::load(path) {
            Ok(c) => (c, None),
            Err(e) => {
                let bak = path.with_extension(format!(
                    "bad.{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                ));
                let _ = std::fs::rename(path, &bak);
                let cfg = Config::default();
                let _ = cfg.save_to(path);
                (
                    cfg,
                    Some(format!(
                        "config broken ({e}), moved to {} and recreated defaults",
                        bak.display()
                    )),
                )
            }
        }
    }

    pub fn save_to(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| format!("mkdir config dir: {e}"))?;
            }
        }
        let text =
            serde_json::to_string_pretty(self).map_err(|e| format!("serialize config: {e}"))?;
        let tmp = path.with_extension("new");
        std::fs::write(&tmp, text.as_bytes()).map_err(|e| format!("write tmp config: {e}"))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("replace config: {e}"))?;
        Ok(())
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.search_roots.is_empty() {
            return Err("search_roots must not be empty".to_string());
        }
        if self.max_depth == 0 || self.max_depth > 12 {
            return Err("max_depth must be 1..=12".to_string());
        }
        if self.maa_api_bases.is_empty() {
            return Err("maa_api_bases must not be empty".to_string());
        }
        for b in &self.maa_api_bases {
            if !(b.starts_with("http://") || b.starts_with("https://")) {
                return Err(format!("bad api base: {b}"));
            }
        }
        if !(self.resource_repo.starts_with("http://")
            || self.resource_repo.starts_with("https://"))
        {
            return Err("resource_repo must be http(s)".to_string());
        }
        self.web_bind
            .parse::<std::net::SocketAddr>()
            .map_err(|e| format!("bad web_bind: {e}"))?;
        if self.launch_stagger_secs > 300 {
            return Err("launch_stagger_secs must be <= 300".to_string());
        }
        if self.auto_prepare_hours > 168 {
            return Err("auto_prepare_hours must be <= 168".to_string());
        }
        if !self.proxy.trim().is_empty() {
            crate::downloader::build_client(self.proxy.trim()).map(|_| ())?;
        }
        Ok(())
    }
}
