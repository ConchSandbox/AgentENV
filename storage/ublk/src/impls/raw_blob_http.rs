use anyhow::{bail, Context, Result};
use reqwest::{header, Client, StatusCode, Url};
use std::time::Duration;

use super::raw_blob_target::RAW_BLOB_MAX_IO_BYTES;

/// A trusted immutable HTTP object. Construction performs no network I/O.
/// Requests fill only the caller's bounded destination; no cache holes exist.
pub(super) struct HttpRangeSource {
    client: Client,
    url: Url,
    size_bytes: u64,
}

impl HttpRangeSource {
    pub(super) fn new(url: &str, size_bytes: u64) -> Result<Self> {
        let url = Url::parse(url).context("invalid raw blob URL")?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            bail!("raw blob URL must use HTTP or HTTPS and include a host");
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .build()
            .context("build raw blob HTTP client")?;
        Ok(Self {
            client,
            url,
            size_bytes,
        })
    }

    pub(super) async fn read(&self, offset: u64, dst: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(dst.len() as u64)
            .context("raw blob range overflow")?;
        if dst.is_empty() || dst.len() > RAW_BLOB_MAX_IO_BYTES as usize || end > self.size_bytes {
            bail!("raw blob HTTP range exceeds request or object bounds");
        }
        let mut response = self
            .client
            .get(self.url.clone())
            .header(header::RANGE, format!("bytes={offset}-{}", end - 1))
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("request raw blob range")?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            bail!(
                "raw blob range returned HTTP {} instead of 206",
                response.status()
            );
        }
        let expected_range = format!("bytes {offset}-{}/{}", end - 1, self.size_bytes);
        if response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            != Some(expected_range.as_str())
        {
            bail!("raw blob Content-Range does not match requested range and size");
        }
        if let Some(encoding) = response.headers().get(header::CONTENT_ENCODING) {
            if encoding != "identity" {
                bail!("raw blob response must not be encoded");
            }
        }
        if let Some(length) = response.headers().get(header::CONTENT_LENGTH) {
            if length.to_str().ok().and_then(|s| s.parse::<u64>().ok()) != Some(dst.len() as u64) {
                bail!("raw blob Content-Length does not match requested length");
            }
        }
        let mut done = 0;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(reqwest::Error::without_url)
            .context("read raw blob range body")?
        {
            if chunk.len() > dst.len() - done {
                bail!("raw blob range body is too long");
            }
            dst[done..done + chunk.len()].copy_from_slice(&chunk);
            done += chunk.len();
        }
        if done != dst.len() {
            bail!("raw blob range body is short");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn serve(
        response: Vec<u8>,
    ) -> (String, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/blob?secret=do-not-log",
            listener.local_addr().unwrap()
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                    assert!(request.len() < 8192);
                }
                seen.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request).to_string());
                let _ = stream.write_all(&response).await;
            }
        });
        (url, requests, task)
    }

    #[tokio::test]
    async fn setup_is_lazy_and_reads_only_requested_ranges() {
        let bytes = vec![37u8; 4096];
        let mut response = b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 8192-12287/16384\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n".to_vec();
        response.extend_from_slice(&bytes);
        let (url, requests, task) = serve(response).await;
        let source = HttpRangeSource::new(&url, 16384).unwrap();
        assert!(requests.lock().unwrap().is_empty());
        let mut dst = vec![0; 4096];
        source.read(8192, &mut dst).await.unwrap();
        assert_eq!(dst, bytes);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]
            .to_lowercase()
            .contains("range: bytes=8192-12287\r\n"));
        assert!(requests[0]
            .to_lowercase()
            .contains("accept-encoding: identity\r\n"));
        task.abort();
    }

    #[tokio::test]
    async fn bad_delivery_never_succeeds_or_returns_false_zeroes() {
        for response in [
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 302 Found\r\nLocation: /redirect\r\nContent-Length: 0\r\n\r\n",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-4095/8192\r\nContent-Length: 4096\r\n\r\n",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4096-8191/8192\r\nContent-Length: 4096\r\nConnection: close\r\n\r\nshort",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4096-8191/8192\r\nContent-Length: 8192\r\n\r\n",
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 4096-8191/8192\r\nContent-Encoding: gzip\r\nContent-Length: 4096\r\n\r\n",
        ] {
            let (url, requests, task) = serve(response.as_bytes().to_vec()).await;
            let source = HttpRangeSource::new(&url, 8192).unwrap();
            let mut dst = vec![99; 4096];
            let error = source.read(4096, &mut dst).await.unwrap_err();
            assert!(!format!("{error:#}").contains("do-not-log"));
            assert_eq!(requests.lock().unwrap().len(), 1, "must not retry or follow redirects");
            assert!(dst.iter().any(|b| *b == 99), "must not fill missing bytes with zeros");
            task.abort();
        }
    }

    #[tokio::test]
    async fn bounds_fail_before_network_and_oversized_chunked_body_fails() {
        let mut response = b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-4095/4096\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1001\r\n".to_vec();
        response.extend(vec![1; 4097]);
        response.extend_from_slice(b"\r\n0\r\n\r\n");
        let (url, requests, task) = serve(response).await;
        let source = HttpRangeSource::new(&url, 4096).unwrap();
        assert!(source.read(u64::MAX, &mut [0; 4096]).await.is_err());
        assert!(source.read(4096, &mut [0; 4096]).await.is_err());
        assert!(source.read(0, &mut []).await.is_err());
        assert!(requests.lock().unwrap().is_empty());
        assert!(source.read(0, &mut [0; 4096]).await.is_err());
        task.abort();
    }
}
