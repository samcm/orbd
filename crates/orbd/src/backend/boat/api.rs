//! The few Boat API calls orbd makes (<https://docs.boat.dev/api/v1>). Responses are
//! parsed only as far as orbd reads them; the API key never appears in errors or logs.

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;

/// Requests that do not run a command finish well within this.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    /// No answer (connect, TLS, timeout, unreadable body). The request may have run.
    #[error("boat API unreachable: {0}")]
    Transport(String),
    #[error("boat API {status} {code}: {message}")]
    Http {
        status: u16,
        code: String,
        message: String,
        /// `error.details.retryable` (502 `sandbox_direct_failed`): true when the call
        /// provably did not run.
        retryable: bool,
    },
}

impl ApiError {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Http { status, .. } => Some(*status),
            Self::Transport(_) => None,
        }
    }

    /// Worth repeating as is: the call did not take effect, or repeating it is harmless
    /// (every call orbd retries is idempotent or reports `retryable`).
    pub fn transient(&self) -> bool {
        match self {
            Self::Transport(_) => true,
            Self::Http {
                status,
                code,
                retryable,
                ..
            } => {
                *retryable
                    || matches!(*status, 500 | 503 | 504)
                    || matches!(
                        code.as_str(),
                        "sandbox_starting"
                            | "boat_starting"
                            | "sandbox_resuming"
                            | "sandbox_stopping"
                            | "sandbox_securing"
                            | "idempotency_in_progress"
                    )
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Sandbox {
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub error: Option<String>,
    /// Set once Boat deleted the disk of a long-stopped sandbox (14 days).
    #[serde(default)]
    pub data_expired_at: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub timed_out: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandStatus {
    pub running: bool,
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
}

/// What a new sandbox is created with. The same value must be sent on every retry of
/// one idempotency key.
pub struct CreateSandbox<'a> {
    pub size: &'a str,
    pub name: &'a str,
    pub labels: &'a [(&'a str, String)],
    pub auto_stop_secs: Option<u64>,
}

pub struct Client {
    http: reqwest::Client,
    base: String,
    key: SecretString,
}

impl Client {
    pub fn new(base: &str, key: SecretString) -> anyhow::Result<Self> {
        // reqwest is built without a bundled provider; use ring like the rest of orbd.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = reqwest::Client::builder()
            .user_agent(concat!("orbd/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
            key,
        })
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, String)],
        headers: &[(&str, String)],
        body: Option<Value>,
        timeout: Duration,
    ) -> Result<Value, ApiError> {
        let mut req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(self.key.expose_secret())
            .query(query)
            .timeout(timeout);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ApiError::Transport(e.without_url().to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| ApiError::Transport(e.without_url().to_string()))?;
        let value: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(value);
        }
        let str_at = |p: &str| value.pointer(p).and_then(Value::as_str);
        Err(ApiError::Http {
            status: status.as_u16(),
            code: str_at("/code")
                .or(str_at("/error/code"))
                .unwrap_or("")
                .to_owned(),
            message: str_at("/message")
                .or(str_at("/error/message"))
                .map(str::to_owned)
                .unwrap_or_else(|| text.chars().take(300).collect()),
            retryable: value
                .pointer("/error/details/retryable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    fn parse<T: for<'de> Deserialize<'de>>(v: Value, field: Option<&str>) -> Result<T, ApiError> {
        let v = match field {
            Some(f) => v.get(f).cloned().unwrap_or(Value::Null),
            None => v,
        };
        serde_json::from_value(v)
            .map_err(|e| ApiError::Transport(format!("unexpected boat API response: {e}")))
    }

    /// Every sandbox carrying all of `labels`, in any state.
    pub async fn list(&self, labels: &[(&str, String)]) -> Result<Vec<Sandbox>, ApiError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut query: Vec<(&str, String)> = labels
                .iter()
                .map(|(k, v)| ("label", format!("{k}={v}")))
                .collect();
            query.push(("limit", "200".into()));
            if let Some(c) = &cursor {
                query.push(("cursor", c.clone()));
            }
            let v = self
                .call(
                    reqwest::Method::GET,
                    "/sandboxes",
                    &query,
                    &[],
                    None,
                    REQUEST_TIMEOUT,
                )
                .await?;
            cursor = v
                .pointer("/pageInfo/nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let more = v
                .pointer("/pageInfo/hasMore")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            out.extend(Self::parse::<Vec<Sandbox>>(v, Some("sandboxes"))?);
            if !more || cursor.is_none() {
                return Ok(out);
            }
        }
    }

    /// None when Boat no longer knows the sandbox.
    pub async fn get(&self, id: &str) -> Result<Option<Sandbox>, ApiError> {
        match self
            .call(
                reqwest::Method::GET,
                &format!("/sandboxes/{id}"),
                &[],
                &[],
                None,
                REQUEST_TIMEOUT,
            )
            .await
        {
            Ok(v) => Self::parse(v, Some("sandbox")).map(Some),
            Err(e) if e.status() == Some(404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Creates a sandbox with no account secrets. Retrying with the same
    /// `idempotency_key` and request returns the same sandbox.
    pub async fn create(
        &self,
        idempotency_key: &str,
        req: &CreateSandbox<'_>,
    ) -> Result<Sandbox, ApiError> {
        let labels: serde_json::Map<String, Value> = req
            .labels
            .iter()
            .map(|(k, v)| ((*k).to_owned(), Value::String(v.clone())))
            .collect();
        let body = json!({
            "type": req.size,
            "name": req.name,
            "labels": labels,
            "noEnv": true,
            "ttlSeconds": req.auto_stop_secs,
        });
        let v = self
            .call(
                reqwest::Method::POST,
                "/sandboxes",
                &[],
                &[("Idempotency-Key", idempotency_key.to_owned())],
                Some(body),
                REQUEST_TIMEOUT,
            )
            .await?;
        Self::parse(v, Some("sandbox"))
    }

    pub async fn resume(&self, id: &str, auto_stop_secs: Option<u64>) -> Result<(), ApiError> {
        self.call(
            reqwest::Method::POST,
            &format!("/sandboxes/{id}/resume"),
            &[],
            &[],
            Some(json!({ "ttlSeconds": auto_stop_secs })),
            REQUEST_TIMEOUT,
        )
        .await
        .map(drop)
    }

    /// Starts (or joins the pending) stop: Boat saves the disk, then removes the machine.
    pub async fn stop(&self, id: &str) -> Result<(), ApiError> {
        self.call(
            reqwest::Method::POST,
            &format!("/sandboxes/{id}/stop"),
            &[],
            &[],
            Some(json!({})),
            REQUEST_TIMEOUT,
        )
        .await
        .map(drop)
    }

    /// Irreversibly deletes the sandbox and its snapshots. A sandbox Boat no longer
    /// knows counts as deleted.
    pub async fn delete(&self, id: &str) -> Result<(), ApiError> {
        match self
            .call(
                reqwest::Method::DELETE,
                &format!("/sandboxes/{id}"),
                &[],
                &[("X-Ascii-Confirm-Delete", id.to_owned())],
                None,
                REQUEST_TIMEOUT,
            )
            .await
        {
            Err(e) if e.status() == Some(404) => Ok(()),
            r => r.map(drop),
        }
    }

    /// Writes `content` to `path` (under /home/user or /tmp).
    pub async fn write_file(&self, id: &str, path: &str, content: &[u8]) -> Result<(), ApiError> {
        use base64::Engine;
        let body = json!({
            "path": path,
            "content": base64::engine::general_purpose::STANDARD.encode(content),
            "encoding": "base64",
        });
        self.call(
            reqwest::Method::PUT,
            &format!("/sandboxes/{id}/files"),
            &[],
            &[],
            Some(body),
            Duration::from_secs(90),
        )
        .await
        .map(drop)
    }

    /// Runs `command` to completion (at most `timeout_secs`, up to 600).
    pub async fn run(
        &self,
        id: &str,
        command: &str,
        timeout_secs: u64,
    ) -> Result<CommandResult, ApiError> {
        let timeout_secs = timeout_secs.clamp(1, 600);
        let v = self
            .call(
                reqwest::Method::POST,
                &format!("/sandboxes/{id}/commands"),
                &[],
                &[],
                Some(json!({ "command": command, "timeoutSeconds": timeout_secs })),
                // A starting sandbox makes the call wait up to 60 s first.
                Duration::from_secs(timeout_secs + 75),
            )
            .await?;
        Self::parse(v, None)
    }

    /// Starts `command` in the background with no time limit; returns its process id.
    pub async fn start_detached(&self, id: &str, command: &str) -> Result<u64, ApiError> {
        let v = self
            .call(
                reqwest::Method::POST,
                &format!("/sandboxes/{id}/commands"),
                &[],
                &[],
                Some(json!({ "command": command, "detached": true })),
                Duration::from_secs(90),
            )
            .await?;
        v.get("processId").and_then(Value::as_u64).ok_or_else(|| {
            ApiError::Transport("boat API: detached command has no processId".into())
        })
    }

    pub async fn command_status(&self, id: &str, process: u64) -> Result<CommandStatus, ApiError> {
        let v = self
            .call(
                reqwest::Method::GET,
                &format!("/sandboxes/{id}/commands/{process}"),
                &[("tailBytes", "4096".into())],
                &[],
                None,
                REQUEST_TIMEOUT,
            )
            .await?;
        Self::parse(v, None)
    }
}
