use std::path::Path;

pub struct ExeVersion {
    pub raw: String,
    pub normalized: Option<semver::Version>,
}

pub fn read_maa_version(exe: &Path) -> ExeVersion {
    let raw = read_product_version(exe).unwrap_or_else(|| "unknown".to_string());
    let normalized = normalize(&raw);
    ExeVersion { raw, normalized }
}

pub fn normalize(raw: &str) -> Option<semver::Version> {
    let s = raw.trim();
    let s = s
        .strip_prefix('v')
        .or_else(|| s.strip_prefix('V'))
        .unwrap_or(s);
    let s = s.split('+').next().unwrap_or(s).trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(v) = semver::Version::parse(s) {
        return Some(v);
    }
    // tolerate 4-part Windows file versions (10.0.26100.9278 -> 10.0.26100)
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit())) {
        return semver::Version::parse(&parts[..3].join(".")).ok();
    }
    None
}

fn read_product_version(exe: &Path) -> Option<String> {
    let bytes = std::fs::read(exe).ok()?;
    let file = pelite::pe64::PeFile::from_bytes(&bytes).ok()?;
    use pelite::pe64::Pe;
    let rsrc = file.resources().ok()?;
    let vi = rsrc.version_info().ok()?;
    for lang in vi.translation() {
        if let Some(v) = vi.value(*lang, "ProductVersion") {
            if !v.trim().is_empty() {
                return Some(v);
            }
        }
    }
    for lang in vi.translation() {
        if let Some(v) = vi.value(*lang, "FileVersion") {
            if !v.trim().is_empty() {
                return Some(v);
            }
        }
    }
    let f = vi.fixed()?;
    Some(f.dwProductVersion.to_string())
}
