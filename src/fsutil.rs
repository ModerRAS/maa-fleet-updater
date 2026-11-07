//! Small filesystem helpers with human-readable errors.
//! `create_dir_all` on an empty path surfaces as OS EINVAL ("invalid argument");
//! refuse early with a message that points at the misconfigured setting.

pub fn ensure_parent(path: &std::path::Path, what: &str) -> Result<(), String> {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => {
            std::fs::create_dir_all(p).map_err(|e| format!("mkdir {what} {}: {e}", p.display()))
        }
        _ => Err(format!(
            "{what}: refusing to create empty parent dir for {}; check staging_dir in config",
            path.display()
        )),
    }
}

pub async fn ensure_parent_async(path: &std::path::Path, what: &str) -> Result<(), String> {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => tokio::fs::create_dir_all(p)
            .await
            .map_err(|e| format!("mkdir {what} {}: {e}", p.display())),
        _ => Err(format!(
            "{what}: refusing to create empty parent dir for {}; check staging_dir in config",
            path.display()
        )),
    }
}
