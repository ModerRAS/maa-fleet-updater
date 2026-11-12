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

/// Strip the Windows verbatim prefix (`\\?\C:\...`, `\\?\UNC\...`).
/// `std::fs::canonicalize` returns verbatim paths; external tools spawned
/// by us (git, mklink) do not understand them and fail with
/// "Invalid argument". Native Rust fs calls accept both, so normalizing
/// everywhere is safe and idempotent.
pub fn from_verbatim(p: &std::path::Path) -> std::path::PathBuf {
    let s = p.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
        return std::path::PathBuf::from(format!(r"\\{rest}"));
    }
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        return std::path::PathBuf::from(rest);
    }
    p.to_path_buf()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_verbatim_prefix() {
        // The exact shape canonicalize() produced on the MAA box.
        assert_eq!(
            from_verbatim(std::path::Path::new(r"\\?\C:\Software\x")),
            std::path::PathBuf::from(r"C:\Software\x")
        );
        assert_eq!(
            from_verbatim(std::path::Path::new(r"\\?\UNC\srv\sh\x")),
            std::path::PathBuf::from(r"\\srv\sh\x")
        );
        assert_eq!(
            from_verbatim(std::path::Path::new(r"C:\Software\x")),
            std::path::PathBuf::from(r"C:\Software\x")
        );
    }
}
