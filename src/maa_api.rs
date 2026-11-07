//! Version check against maaApi (metadata only) with GitHub-first asset URLs.
//! Mirrors MAA's GetVersionDetailsByMaaApi asset-pick rules:
//! full = name contains maa-{latest}-, OTA = contains ota + {cur}_{latest}.
//! DebugSymbol assets are excluded. GitHub browser_download_url is primary,
//! maaApi mirrors are fallback. OTA is preferred; full is the fallback option.

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct AssetPick {
    pub kind: String,
    pub file_name: String,
    pub urls: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CheckOk {
    pub latest_raw: String,
    pub ota: Option<AssetPick>,
    pub full: Option<AssetPick>,
}

pub enum CheckRes {
    UpToDate,
    Available(CheckOk),
}

fn norm(raw: &str) -> Option<semver::Version> {
    crate::pe_version::normalize(raw)
}

async fn get_json(client: &reqwest::Client, url: &str) -> Result<Value, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    resp.json::<Value>()
        .await
        .map_err(|e| format!("parse {url}: {e}"))
}

/// Check one (channel, current version) group. Pure metadata fetch (tiny JSON).
pub async fn check(
    client: &reqwest::Client,
    bases: &[String],
    channel: &str,
    cur_raw: &str,
    cur: &semver::Version,
    arch_arm: bool,
) -> Result<CheckRes, String> {
    let mut summary_err = String::new();
    let (summary, base) = {
        let mut hit: Option<(Value, String)> = None;
        for b in bases {
            let url = format!("{}/version/summary.json", b.trim_end_matches('/'));
            match get_json(client, &url).await {
                Ok(j) => {
                    hit = Some((j, b.clone()));
                    break;
                }
                Err(e) => {
                    summary_err = e;
                }
            }
        }
        hit.ok_or(summary_err)?
    };
    let entry = summary.get(channel).cloned().unwrap_or(Value::Null);
    let latest_raw = entry
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if latest_raw.is_empty() {
        return Err(format!("channel {channel} missing in summary"));
    }
    let latest = norm(&latest_raw).ok_or_else(|| format!("bad latest version {latest_raw}"))?;
    if latest <= *cur {
        return Ok(CheckRes::UpToDate);
    }
    let detail_url = entry
        .get("detail")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{}/version/{}.json", base.trim_end_matches('/'), channel));
    let detail = get_json(client, &detail_url).await?;
    let assets = detail
        .pointer("/details/assets")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let latest_l = latest_raw.to_lowercase();
    let cur_l = cur_raw.to_lowercase();
    let full_prefix = format!("maa-{}-", latest_l);
    let ota_need = format!("{}_{}", cur_l, latest_l);
    let mut ota: Option<AssetPick> = None;
    let mut full: Option<AssetPick> = None;
    for a in &assets {
        let name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name.is_empty() {
            continue;
        }
        let nl = name.to_lowercase();
        if !nl.contains("win") {
            continue;
        }
        if nl.contains("debugsymbol") {
            continue;
        }
        if arch_arm != nl.contains("arm") {
            continue;
        }
        let raw_url = a
            .get("browser_download_url")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if raw_url.is_empty() {
            continue;
        }
        let mut urls = vec![raw_url.to_string()];
        if let Some(m) = a.get("mirrors").and_then(|v| v.as_array()) {
            for x in m {
                if let Some(s) = x.as_str() {
                    if !s.is_empty() {
                        urls.push(s.to_string());
                    }
                }
            }
        }
        if ota.is_none() && nl.contains("ota") && nl.contains(&ota_need) {
            ota = Some(AssetPick {
                kind: "ota".to_string(),
                file_name: name.to_string(),
                urls,
            });
            continue;
        }
        if full.is_none() && nl.contains(&full_prefix) {
            full = Some(AssetPick {
                kind: "full".to_string(),
                file_name: name.to_string(),
                urls,
            });
        }
    }
    Ok(CheckRes::Available(CheckOk {
        latest_raw,
        ota,
        full,
    }))
}
