//! Manage this updater\u0027s own autostart via the Windows Task Scheduler.
//! Runs synchronously (spawn_blocking at call sites). No MAA logic here.

pub const TASK_NAME: &str = "MAA Fleet Updater";

fn schtasks(args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("schtasks")
        .args(args)
        .output()
        .map_err(|e| format!("spawn schtasks: {e}"))?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    if out.status.success() {
        Ok(text.trim().to_string())
    } else {
        Err(text.trim().to_string())
    }
}

pub fn status() -> (bool, String) {
    match schtasks(&["/query", "/tn", TASK_NAME, "/fo", "LIST", "/v"]) {
        Ok(t) => (true, t),
        Err(t) => (
            false,
            if t.is_empty() {
                "not registered".to_string()
            } else {
                t
            },
        ),
    }
}

/// Register a logon-triggered task that starts this updater with its config.
/// /f makes it idempotent. Runs as the current user (no admin needed).
pub fn enable(exe: &std::path::Path, cfg: &std::path::Path) -> Result<String, String> {
    let tr = format!("\"{}\" --config \"{}\"", exe.display(), cfg.display());
    schtasks(&[
        "/create", "/tn", TASK_NAME, "/tr", &tr, "/sc", "onlogon", "/delay", "0000:10", "/f",
    ])
}

pub fn disable() -> Result<String, String> {
    match schtasks(&["/delete", "/tn", TASK_NAME, "/f"]) {
        Ok(t) => Ok(t),
        Err(t) if t.to_lowercase().contains("cannot find") || t.contains("0x80070002") => {
            Ok("already absent".to_string())
        }
        Err(t) => Err(t),
    }
}
