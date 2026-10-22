//! Narrow QMP client. The pinned `qapi` crate provides the transport (greeting,
//! capability negotiation, JSON framing, response matching); the commands and the
//! response fields orbd reads are declared here, so schema drift between `qapi`'s
//! generated types and the host's QEMU (9.0 or newer) cannot break decoding.

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
    channels: [Channel; 1],
}
command!(Migrate, "migrate", qapi::Empty);
#[derive(Serialize)]
struct MigrateIncoming {
    channels: [Channel; 1],
}
command!(MigrateIncoming, "migrate-incoming", qapi::Empty);
/// A `file` migration channel: structured, so the path needs no URI escaping.
#[derive(Serialize)]
struct Channel {
    #[serde(rename = "channel-type")]
    channel_type: &'static str,
    addr: FileAddr,
}
#[derive(Serialize)]
struct FileAddr {
    transport: &'static str,
    filename: String,
    offset: u64,
}
impl Channel {
    fn file(path: &Path) -> [Self; 1] {
        [Self {
            channel_type: "main",
            addr: FileAddr {
                transport: "file",
                filename: path.display().to_string(),
                offset: 0,
            },
        }]
    }
}

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

/// The migration parameters orbd sets; `None` is left unset (and unread).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Parameters {
    #[serde(rename = "max-bandwidth", skip_serializing_if = "Option::is_none")]
    pub max_bandwidth: Option<u64>,
    #[serde(rename = "multifd-channels", skip_serializing_if = "Option::is_none")]
    pub multifd_channels: Option<u8>,
    /// Only sent when on: QEMU 9.0 has no `direct-io`.
    #[serde(rename = "direct-io", skip_serializing_if = "Option::is_none")]
    pub direct_io: Option<bool>,
}
command!(Parameters, "migrate-set-parameters", qapi::Empty);

#[derive(Serialize)]
struct QueryParameters {}
command!(QueryParameters, "query-migrate-parameters", Parameters);

/// How a checkpoint's memory file is written; recorded in its manifest and applied
/// again, on the loading QEMU, to read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileMigration {
    /// multifd channels writing and reading RAM in parallel (0: single channel).
    pub multifd_channels: u8,
    /// RAM pages bypass the page cache (needs multifd).
    pub direct_io: bool,
}

/// Capabilities that must be off for a self-contained memory file: no compression or
/// XBZRLE deltas, no auto-converge throttling, no postcopy or return path, and RAM
/// always copied (`x-ignore-shared` off). `mapped-ram` is always on and `multifd` as
/// [`FileMigration`] says.
const OFF_CAPABILITIES: &[&str] = &[
    "xbzrle",
    "compress",
    "auto-converge",
    "postcopy-ram",
    "return-path",
    "x-ignore-shared",
    "release-ram",
    "block",
    "background-snapshot",
    "zero-copy-send",
    "postcopy-preempt",
    "pause-before-switchover",
    "late-block-activate",
    "switchover-ack",
    "dirty-limit",
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

    /// Saves the VM into `path` (QEMU creates or truncates it).
    pub async fn migrate_to_file(&self, path: &Path) -> Result<(), QmpError> {
        self.run(Migrate {
            channels: Channel::file(path),
        })
        .await
        .map(drop)
    }

    /// Loads the VM from `path` (QEMU started with `-incoming defer`).
    pub async fn migrate_incoming_file(&self, path: &Path) -> Result<(), QmpError> {
        self.run(MigrateIncoming {
            channels: Channel::file(path),
        })
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
    /// they took: `mapped-ram` (and `multifd` if asked) on, every other format-changing
    /// capability off, and `max_bandwidth` bytes/s (0: unlimited).
    pub async fn configure_migration(
        &self,
        m: FileMigration,
        max_bandwidth: u64,
    ) -> Result<(), QmpError> {
        let want = |name: &str| match name {
            "mapped-ram" => Some(true),
            "multifd" => Some(m.multifd_channels > 0),
            n if OFF_CAPABILITIES.contains(&n) => Some(false),
            _ => None,
        };
        let current = self.run(QueryCapabilities {}).await?;
        if !current.iter().any(|c| c.capability == "mapped-ram") {
            return Err(QmpError::Command {
                command: "query-migrate-capabilities",
                desc: "this QEMU has no mapped-ram capability (needs QEMU 9.0 or newer)".into(),
            });
        }
        let changes: Vec<CapabilityState> = current
            .iter()
            .filter_map(|c| {
                let state = want(&c.capability)?;
                (state != c.state).then(|| CapabilityState {
                    capability: c.capability.clone(),
                    state,
                })
            })
            .collect();
        if !changes.is_empty() {
            self.run(SetCapabilities {
                capabilities: changes,
            })
            .await?;
        }
        let params = Parameters {
            max_bandwidth: Some(max_bandwidth),
            multifd_channels: (m.multifd_channels > 0).then_some(m.multifd_channels),
            direct_io: m.direct_io.then_some(true),
        };
        self.run(params.clone()).await?;
        let caps = self.run(QueryCapabilities {}).await?;
        if let Some(c) = caps
            .iter()
            .find(|c| want(&c.capability).is_some_and(|w| w != c.state))
        {
            return Err(QmpError::Command {
                command: "migrate-set-capabilities",
                desc: format!("{} is {}", c.capability, if c.state { "on" } else { "off" }),
            });
        }
        let now = self.run(QueryParameters {}).await?;
        if now.max_bandwidth != params.max_bandwidth
            || now.direct_io.unwrap_or(false) != m.direct_io
            || (m.multifd_channels > 0 && now.multifd_channels != params.multifd_channels)
        {
            return Err(QmpError::Command {
                command: "migrate-set-parameters",
                desc: format!("wanted {params:?}, QEMU has {now:?}"),
            });
        }
        Ok(())
    }
}
