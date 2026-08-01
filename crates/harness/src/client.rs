//! Minimal HTTP driver for the service's wire surface (spec 14).

use data_service_core::types::BlockRef;

pub struct Client {
    base: String,
    http: reqwest::Client,
}

/// A captured response of any GET route.
#[derive(Debug)]
pub struct RawResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: bytes::Bytes,
}

/// A captured `/stream` response, undecoded.
#[derive(Debug)]
pub struct StreamResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub vary: Option<String>,
    pub finalized_number: Option<String>,
    pub finalized_hash: Option<String>,
    pub body: bytes::Bytes,
}

impl Client {
    pub fn new(port: u16) -> Self {
        Client {
            base: format!("http://127.0.0.1:{port}"),
            // Auto-decompression off: validators need the raw wire payload,
            // and the body is concatenated per-block members reqwest's
            // single-member decoder cannot handle anyway.
            // Bounded: a request must outlive the SUT's 5 s wait (RP-4), but
            // an unbounded-wait regression must fail the test, not hang it.
            http: reqwest::Client::builder()
                .no_gzip()
                .connect_timeout(std::time::Duration::from_secs(2))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    pub async fn head(&self) -> anyhow::Result<BlockRef> {
        Ok(self
            .http
            .get(format!("{}/head", self.base))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn finalized_head(&self) -> anyhow::Result<BlockRef> {
        Ok(self
            .http
            .get(format!("{}/finalized-head", self.base))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    /// Raw GET for binding checks: status, content type and body.
    pub async fn get_raw(&self, path: &str) -> anyhow::Result<RawResponse> {
        let resp = self.http.get(format!("{}{path}", self.base)).send().await?;
        Ok(RawResponse {
            status: resp.status().as_u16(),
            content_type: resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string),
            body: resp.bytes().await?,
        })
    }

    pub async fn stream(
        &self,
        from_block: u64,
        parent_block_hash: Option<&str>,
        accept_encoding: &str,
    ) -> anyhow::Result<StreamResponse> {
        let mut body = serde_json::json!({ "fromBlock": from_block });
        if let Some(ph) = parent_block_hash {
            body["parentBlockHash"] = serde_json::Value::String(ph.to_string());
        }
        let resp = self
            .http
            .post(format!("{}/stream", self.base))
            .header("accept-encoding", accept_encoding)
            .json(&body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let h = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        };
        let content_type = h("content-type");
        let content_encoding = h("content-encoding");
        let vary = h("vary");
        let finalized_number = h("x-sqd-finalized-head-number");
        let finalized_hash = h("x-sqd-finalized-head-hash");
        Ok(StreamResponse {
            status,
            content_type,
            content_encoding,
            vary,
            finalized_number,
            finalized_hash,
            body: resp.bytes().await?,
        })
    }
}
