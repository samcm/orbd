//! Narrow QMP client. The pinned `qapi` crate provides the transport (greeting,
//! capability negotiation, JSON framing, response matching); the commands and the
//! response fields orbd reads are declared here, so schema drift between `qapi`'s
//! generated types and the host's QEMU (7.2 on Debian 12) cannot break decoding.

use qapi::futures::{QapiService, QmpStreamTokio};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use tokio::io::WriteHalf;
use tokio::net::UnixStream;

type Writer = QmpStreamTokio<WriteHalf<UnixStream>>;

#[derive(Debug, thiserror::Error)]
pub enum QmpError {
    #[error("QMP I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("QMP {command}: {desc}")]
    Command { command: &'static str, desc: String },
    #[error("QMP connect timed out (socket busy or QEMU hung)")]
    Timeout,
}

/// One QMP session. Drop it promptly: QEMU serves one client per monitor socket.
pub struct Qmp {
    service: QapiService<Writer>,
    events: tokio::task::JoinHandle<()>,
}

impl Drop for Qmp {
    fn drop(&mut self) {
        self.events.abort();
    }
}

macro_rules! command {
    ($ty:ident, $name:literal, $ok:ty) => {
        impl qapi::Command for $ty {
            type Ok = $ok;
            const NAME: &'static str = $name;
            const ALLOW_OOB: bool = false;
        }
        impl qapi::qmp::QmpCommand for $ty {}
    };
}

#[derive(Serialize)]
struct Stop {}
command!(Stop, "stop", qapi::Empty);
#[derive(Serialize)]
struct Cont {}
command!(Cont, "cont", qapi::Empty);
#[derive(Serialize)]
struct Quit {}
command!(Quit, "quit", qapi::Empty);
#[derive(Serialize)]
struct SystemPowerdown {}
command!(SystemPowerdown, "system_powerdown", qapi::Empty);
#[derive(Serialize)]
struct MigrateCancel {}
command!(MigrateCancel, "migrate_cancel", qapi::Empty);
#[derive(Serialize)]
struct Migrate {
    uri: String,
}
command!(Migrate, "migrate", qapi::Empty);
#[derive(Serialize)]
struct MigrateIncoming {
    uri: String,
}
command!(MigrateIncoming, "migrate-incoming", qapi::Empty);

#[derive(Serialize)]
struct QueryStatus {}
command!(QueryStatus, "query-status", StatusInfo);
#[derive(Deserialize)]
pub struct StatusInfo {
    pub status: String,
}

#[derive(Serialize)]
struct QueryName {}
command!(QueryName, "query-name", NameInfo);
#[derive(Deserialize)]
pub struct NameInfo {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Serialize)]
struct QueryMigrate {}
command!(QueryMigrate, "query-migrate", MigrationInfo);
#[derive(Debug, Default, Deserialize)]
pub struct MigrationInfo {
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, rename = "error-desc")]
    pub error_desc: Option<String>,
    #[serde(default)]
    pub ram: Option<RamStats>,
}
#[derive(Debug, Default, Deserialize)]
pub struct RamStats {
    #[serde(default)]
    pub transferred: u64,
}

#[derive(Serialize)]
struct QueryCapabilities {}
command!(
    QueryCapabilities,
    "query-migrate-capabilities",
    Vec<CapabilityState>
);
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapabilityState {
    pub capability: String,
    pub state: bool,
}
#[derive(Serialize)]
struct SetCapabilities {
    capabilities: Vec<CapabilityState>,
}
command!(SetCapabilities, "migrate-set-capabilities", qapi::Empty);

#[derive(Serialize)]
struct SetParameters {
    #[serde(rename = "max-bandwidth")]
    max_bandwidth: u64,
}
command!(SetParameters, "migrate-set-parameters", qapi::Empty);

#[derive(Serialize)]
struct QueryParameters {}
command!(QueryParameters, "query-migrate-parameters", Parameters);
#[derive(Deserialize)]
pub struct Parameters {
    #[serde(default, rename = "max-bandwidth")]
    pub max_bandwidth: Option<u64>,
}

/// Capabilities that must be off for a plain, self-contained memory stream: no
/// compression or XBZRLE deltas, no auto-converge throttling, no postcopy or return
/// path, single channel, and RAM always copied (`x-ignore-shared` off).
const OFF_CAPABILITIES: &[&str] = &[
    "xbzrle",
    "compress",
    "auto-converge",
    "postcopy-ram",
    "return-path",
    "multifd",
    "x-ignore-shared",
    "release-ram",
    "block",
    "background-snapshot",
    "zero-copy-send",
    "postcopy-preempt",
    "pause-before-switchover",
    "late-block-activate",
    "mapped-ram",
];

impl Qmp {
    /// Connects and negotiates, bounded by `timeout`.
    pub async fn connect(path: &Path, timeout: Duration) -> Result<Self, QmpError> {
        let fut = async {
            let neg = QmpStreamTokio::open_uds(path).await?;
            let stream = neg.negotiate().await?;
            let (service, events) = stream.spawn_tokio();
            Ok::<_, std::io::Error>(Self { service, events })
        };
        match tokio::time::timeout(timeout, fut).await {
            Ok(r) => Ok(r?),
            Err(_) => Err(QmpError::Timeout),
        }
    }

    async fn run<C>(&self, c: C) -> Result<C::Ok, QmpError>
    where
        C: qapi::Command + qapi::qmp::QmpCommand,
    {
        self.service.execute(c).await.map_err(|e| match e {
            qapi::ExecuteError::Io(e) => QmpError::Io(e),
            qapi::ExecuteError::Qapi(e) => QmpError::Command {
                command: C::NAME,
                desc: e.desc,
            },
        })
    }

    pub async fn status(&self) -> Result<String, QmpError> {
        Ok(self.run(QueryStatus {}).await?.status)
    }

    pub async fn name(&self) -> Result<Option<String>, QmpError> {
        Ok(self.run(QueryName {}).await?.name)
    }

    pub async fn stop(&self) -> Result<(), QmpError> {
        self.run(Stop {}).await.map(drop)
    }

    pub async fn cont(&self) -> Result<(), QmpError> {
        self.run(Cont {}).await.map(drop)
    }

    /// Asks QEMU to exit. The reply may be lost when QEMU closes the socket first.
    pub async fn quit(&self) -> Result<(), QmpError> {
        match self.run(Quit {}).await {
            Ok(_) | Err(QmpError::Io(_)) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn system_powerdown(&self) -> Result<(), QmpError> {
        self.run(SystemPowerdown {}).await.map(drop)
    }

    pub async fn migrate(&self, uri: &str) -> Result<(), QmpError> {
        self.run(Migrate { uri: uri.into() }).await.map(drop)
    }

    pub async fn migrate_incoming(&self, uri: &str) -> Result<(), QmpError> {
        self.run(MigrateIncoming { uri: uri.into() })
            .await
            .map(drop)
    }

    pub async fn migrate_cancel(&self) -> Result<(), QmpError> {
        self.run(MigrateCancel {}).await.map(drop)
    }

    pub async fn query_migrate(&self) -> Result<MigrationInfo, QmpError> {
        self.run(QueryMigrate {}).await
    }

    /// Applies orbd's migration settings on either side of a save/restore and checks
    /// they took: unlimited bandwidth and every format-changing capability off.
    pub async fn configure_migration(&self) -> Result<(), QmpError> {
        let current = self.run(QueryCapabilities {}).await?;
        let to_clear: Vec<CapabilityState> = current
            .iter()
            .filter(|c| c.state && OFF_CAPABILITIES.contains(&c.capability.as_str()))
            .map(|c| CapabilityState {
                capability: c.capability.clone(),
                state: false,
            })
            .collect();
        if !to_clear.is_empty() {
            self.run(SetCapabilities {
                capabilities: to_clear,
            })
            .await?;
        }
        // 0 disables rate limiting (QEMU's default is 128 MiB/s).
        self.run(SetParameters { max_bandwidth: 0 }).await?;
        let caps = self.run(QueryCapabilities {}).await?;
        let params = self.run(QueryParameters {}).await?;
        if let Some(c) = caps
            .iter()
            .find(|c| c.state && OFF_CAPABILITIES.contains(&c.capability.as_str()))
        {
            return Err(QmpError::Command {
                command: "migrate-set-capabilities",
                desc: format!("{} is still enabled", c.capability),
            });
        }
        if params.max_bandwidth != Some(0) {
            return Err(QmpError::Command {
                command: "migrate-set-parameters",
                desc: format!("max-bandwidth is {:?}", params.max_bandwidth),
            });
        }
        Ok(())
    }
}
