//! Typed client for the parts of Firecracker's API (v1.17 OpenAPI spec) orbd uses:
//! HTTP/1.1 over the VMM's unix socket, one connection per request.
//!
//! Firecracker runs API requests one at a time: a request waits behind an executing
//! one (e.g. a snapshot being written), and dropping a request does not cancel it.

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("Firecracker API: {0}")]
    Io(#[from] std::io::Error),
    #[error("Firecracker API: {0}")]
    Http(#[from] hyper::Error),
    #[error("Firecracker API {method} {path}: {status}: {fault}")]
    Fault {
        method: Method,
        path: String,
        status: StatusCode,
        fault: String,
    },
    #[error("Firecracker API {0} timed out")]
    Timeout(String),
}

pub struct Api {
    socket: PathBuf,
}

/// `GET /` (InstanceInfo).
#[derive(Debug, Deserialize)]
pub struct InstanceInfo {
    pub id: String,
    /// `Not started`, `Running` or `Paused`.
    pub state: String,
}

#[derive(Serialize)]
pub struct MachineConfig {
    pub vcpu_count: u32,
    pub mem_size_mib: u64,
    pub smt: bool,
}

#[derive(Serialize)]
pub struct BootSource<'a> {
    pub kernel_image_path: &'a Path,
    pub boot_args: &'a str,
}

#[derive(Serialize)]
pub struct Drive<'a> {
    pub drive_id: &'a str,
    pub path_on_host: &'a Path,
    pub is_root_device: bool,
    pub is_read_only: bool,
}

#[derive(Serialize)]
pub struct NetworkInterface<'a> {
    pub iface_id: &'a str,
    pub host_dev_name: &'a str,
    pub guest_mac: &'a str,
}

#[derive(Serialize)]
pub struct Vsock<'a> {
    pub guest_cid: u32,
    pub uds_path: &'a Path,
}

#[derive(Serialize)]
pub struct Balloon {
    pub amount_mib: u64,
    pub deflate_on_oom: bool,
    pub free_page_reporting: bool,
}

#[derive(Serialize)]
pub struct SnapshotCreate<'a> {
    pub snapshot_type: &'static str,
    pub snapshot_path: &'a Path,
    pub mem_file_path: &'a Path,
}

#[derive(Serialize)]
pub struct MemBackend<'a> {
    pub backend_type: &'static str,
    pub backend_path: &'a Path,
}

#[derive(Serialize)]
pub struct SnapshotLoad<'a> {
    pub snapshot_path: &'a Path,
    pub mem_backend: MemBackend<'a>,
    pub resume_vm: bool,
}

impl Api {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Bytes, ApiError> {
        let what = format!("{method} {path}");
        let fut = async {
            let stream = tokio::net::UnixStream::connect(&self.socket).await?;
            let (mut sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
            let conn = tokio::spawn(conn);
            let mut req = Request::builder()
                .method(method.clone())
                .uri(path)
                .header("Host", "localhost")
                .header("Accept", "application/json");
            if body.is_some() {
                req = req.header("Content-Type", "application/json");
            }
            let req = req
                .body(Full::new(Bytes::from(body.unwrap_or_default())))
                .expect("valid request");
            let res = sender.send_request(req).await;
            let out = async {
                let res = res?;
                let status = res.status();
                let bytes = res.into_body().collect().await?.to_bytes();
                if status.is_success() {
                    Ok(bytes)
                } else {
                    let fault = serde_json::from_slice::<serde_json::Value>(&bytes)
                        .ok()
                        .and_then(|v| v.get("fault_message")?.as_str().map(str::to_owned))
                        .unwrap_or_else(|| String::from_utf8_lossy(&bytes).into_owned());
                    Err(ApiError::Fault {
                        method,
                        path: path.to_owned(),
                        status,
                        fault,
                    })
                }
            }
            .await;
            conn.abort();
            out
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| ApiError::Timeout(what))?
    }

    pub async fn put<T: Serialize>(
        &self,
        path: &str,
        body: &T,
        timeout: Duration,
    ) -> Result<(), ApiError> {
        let body = serde_json::to_vec(body).expect("serializable");
        self.call(Method::PUT, path, Some(body), timeout)
            .await
            .map(drop)
    }

    /// `PATCH /vm` to `Paused` or `Resumed`.
    pub async fn set_state(&self, state: &str, timeout: Duration) -> Result<(), ApiError> {
        let body = serde_json::to_vec(&serde_json::json!({ "state": state })).expect("json");
        self.call(Method::PATCH, "/vm", Some(body), timeout)
            .await
            .map(drop)
    }

    pub async fn action(&self, action_type: &str, timeout: Duration) -> Result<(), ApiError> {
        self.put(
            "/actions",
            &serde_json::json!({ "action_type": action_type }),
            timeout,
        )
        .await
    }

    pub async fn info(&self, timeout: Duration) -> Result<InstanceInfo, ApiError> {
        let bytes = self.call(Method::GET, "/", None, timeout).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| ApiError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
    }
}
