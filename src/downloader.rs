//! File download with GitHub-first URL list, proxy-aware client, progress events.
//! Used ONLY in the prepare phase (while instances run or anytime before reboot).
//! The commit phase performs zero network requests.

use futures_util::StreamExt;
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub fn build_client(proxy: &str) -> Result<reqwest::Client, String> {
    let mut b = reqwest::Client::builder().connect_timeout(std::time::Duration::from_secs(20));
    if !proxy.trim().is_empty() {
        let p = reqwest::Proxy::all(proxy.trim()).map_err(|e| format!("bad proxy: {e}"))?;
        b = b.proxy(p);
    }
    b.build().map_err(|e| format!("build http client: {e}"))
}

/// Try each URL in order until one succeeds. Returns after first success.
pub async fn download_first(
    client: &reqwest::Client,
    urls: &[String],
    dest: &Path,
    mut on_progress: impl FnMut(u64, Option<u64>),
) -> Result<(), String> {
    crate::fsutil::ensure_parent_async(dest, "download").await?;
    let mut last_err = String::from("no urls");
    for url in urls {
        match try_one(client, url, dest, &mut on_progress).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last_err = format!("{url}: {e}");
            }
        }
    }
    Err(last_err)
}

async fn try_one(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    on_progress: &mut impl FnMut(u64, Option<u64>),
) -> Result<(), String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("request: {e}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    let total = resp.content_length();
    let tmp = dest.with_extension("part");
    let mut f = tokio::fs::File::create(&tmp)
        .await
        .map_err(|e| format!("create file: {e}"))?;
    let mut done: u64 = 0;
    let mut stream = resp.bytes_stream();
    loop {
        // Stall watchdog: 60s without a single chunk -> give up on this URL
        // so a half-dead connection fails over to the next mirror instead of
        // hanging the whole prepare phase forever.
        let next = tokio::time::timeout(std::time::Duration::from_secs(60), stream.next()).await;
        let chunk = match next {
            Err(_) => return Err("stalled: no data for 60s, failing over".to_string()),
            Ok(None) => break,
            Ok(Some(c)) => c,
        };
        let c = chunk.map_err(|e| format!("body: {e}"))?;
        f.write_all(&c).await.map_err(|e| format!("write: {e}"))?;
        done += c.len() as u64;
        on_progress(done, total);
    }
    f.flush().await.map_err(|e| format!("flush: {e}"))?;
    drop(f);
    tokio::fs::rename(&tmp, dest)
        .await
        .map_err(|e| format!("rename: {e}"))?;
    Ok(())
}
