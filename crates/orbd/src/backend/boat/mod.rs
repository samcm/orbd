//! Boat backend: each orb is a Boat sandbox (<https://boat.dev>), driven by the
//! coordinator over Boat's HTTP API. There is no agent: the executor runs in the
//! coordinator, with its receipts in its own SQLite store (`boat.sqlite` in the
//! coordinator's state directory).
//!
//! * Spawn creates the sandbox (or resumes it), then starts `amp orb bootstrap` in it as
//!   `user`. Boat's base machine is used, not Amp's orb image: the Amp CLI is installed
//!   on first spawn.
//! * Pause stops the sandbox. Boat keeps its disk, not its processes, for 14 days; set
//!   `idle_orb_ttl_days` below that. A sandbox whose disk expired anyway is replaced by
//!   a fresh one on its next spawn.
//! * Delete deletes the sandbox and its snapshots.
//!
//! Sandboxes are created with `noEnv` (none of the Boat account's secrets), labelled
//! with the orb's internal id and disk incarnation, and with an idempotency key, so a
//! lost create response is found again instead of creating a second sandbox.

mod api;
mod executor;

use super::{
    Availability, BackendError, BackendHandle, CancelRequest, CapabilityError, CommandDelivery,
    ComputeBackend, PlacementCandidate, PlacementRequest, ReconcilePage, ReconcileQuery,
};
use crate::config::coordinator::{BOAT_BACKEND, BoatConfig};
use crate::model::{
    AllocationId, BackendCapabilities, BackendId, BackendScopeId, CapacityModel, CommandKey,
    DiskReport, ExecutionTarget, IdleCharge, IdleCost, Location, MIB, Resources, ResumeLocality,
    SleepCapability, SleepingCompute, SnapshotCapability, StateLocation, StateStore, Support,
};
use crate::secrets::SecretKey;
use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use tokio_stream::wrappers::UnboundedReceiverStream;

/// One Boat machine size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Size {
    /// Boat's `type`.
    pub name: &'static str,
    pub vcpus: u32,
    pub memory_mib: u64,
}

/// Boat's sizes (<https://docs.boat.dev/machines>). `large` and `xlarge` need a paid plan.
pub const SIZES: [Size; 4] = [
    Size {
        name: "small",
        vcpus: 2,
        memory_mib: 4096,
    },
    Size {
        name: "default",
        vcpus: 4,
        memory_mib: 8192,
    },
    Size {
        name: "large",
        vcpus: 8,
        memory_mib: 16384,
    },
    Size {
        name: "xlarge",
        vcpus: 16,
        memory_mib: 32768,
    },
];

pub fn size_for(vcpus: u32, memory_mib: u64) -> Option<Size> {
    SIZES
        .into_iter()
        .find(|s| s.vcpus == vcpus && s.memory_mib == memory_mib)
}

pub fn describe_sizes() -> String {
    let all: Vec<String> = SIZES
        .iter()
        .map(|s| format!("{} vCPUs/{} MiB ({})", s.vcpus, s.memory_mib, s.name))
        .collect();
    all.join(", ")
}

/// The one capacity pool and scope: the configured Boat account.
const POOL: &str = "boat";

pub struct BoatBackend {
    id: BackendId,
    ceiling: Resources,
    executor: Arc<executor::Executor>,
}

impl BoatBackend {
    /// Opens the backend's store and resumes its unfinished operations.
    pub async fn start(
        config: &BoatConfig,
        state_dir: &std::path::Path,
        key: Arc<SecretKey>,
    ) -> anyhow::Result<BackendHandle> {
        let api_key = std::fs::read_to_string(&config.api_key_file)
            .with_context(|| format!("reading {}", config.api_key_file.display()))?;
        let api_key = api_key.trim();
        if api_key.is_empty() {
            anyhow::bail!("{} is empty", config.api_key_file.display());
        }
        let api = api::Client::new(&config.api_url, api_key.to_owned().into())?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let executor = executor::Executor::open(
            &state_dir.join("boat.sqlite"),
            api,
            key,
            (config.auto_stop_secs > 0).then_some(config.auto_stop_secs),
            tx,
        )?;
        executor.recover().await?;
        let backend = Arc::new(BoatBackend {
            id: BackendId::new(BOAT_BACKEND),
            ceiling: Resources {
                vcpus: config.capacity.vcpus,
                memory_bytes: config.capacity.memory_mib.saturating_mul(MIB),
                // Boat sizes the disks; orbd does not limit them.
                disk_bytes: u64::MAX / 4,
            },
            executor,
        });
        Ok(BackendHandle {
            backend,
            events: UnboundedReceiverStream::new(rx).boxed(),
        })
    }

    fn check_location(&self, location: &Location) -> Result<(), BackendError> {
        match location {
            Location::External { backend_id, .. } if backend_id == &self.id => Ok(()),
            other => Err(BackendError::Invalid(format!(
                "location {other:?} does not belong to backend {}",
                self.id
            ))),
        }
    }
}

fn capabilities() -> BackendCapabilities {
    BackendCapabilities {
        // A stopped sandbox keeps its disk, not its processes, and bills no compute.
        sleep: SleepCapability::DiskOnly {
            compute: SleepingCompute::Released,
        },
        nested_kvm: Support::Supported,
        project_snapshots: SnapshotCapability::Unsupported,
        capacity: CapacityModel::QuotaLimited,
        resume_locality: ResumeLocality::BackendManaged,
        state_location: StateLocation {
            disk: StateStore::ProviderManaged,
            sleep_checkpoint: StateStore::None,
        },
        idle_cost: IdleCost {
            compute: IdleCharge::None,
            storage: IdleCharge::None,
        },
    }
}

#[async_trait]
impl ComputeBackend for BoatBackend {
    fn id(&self) -> &BackendId {
        &self.id
    }

    fn capabilities(
        &self,
        target: &ExecutionTarget,
    ) -> Result<BackendCapabilities, CapabilityError> {
        match target {
            ExecutionTarget::Provider { profile } if SIZES.iter().any(|s| s.name == profile.0) => {
                Ok(capabilities())
            }
            other => Err(CapabilityError(format!(
                "the boat backend cannot serve {other:?}"
            ))),
        }
    }

    async fn candidates(
        &self,
        request: PlacementRequest,
    ) -> Result<Vec<PlacementCandidate>, BackendError> {
        let location = match request.existing {
            Some(loc) if self.check_location(&loc).is_ok() => loc,
            Some(_) => return Ok(Vec::new()),
            None => Location::External {
                backend_id: self.id.clone(),
                scope: BackendScopeId::new(POOL),
                allocation_id: AllocationId::generate(),
            },
        };
        Ok(vec![PlacementCandidate {
            location,
            availability: Availability::Available,
            capabilities: capabilities(),
            capacity_pool: POOL.into(),
            capacity_ceiling: self.ceiling,
            demand: request.resources,
            memory: None,
            disk: Some(DiskReport {
                free_bytes: self.ceiling.disk_bytes,
                total_bytes: self.ceiling.disk_bytes,
                allocated_bytes: 0,
                reserve_bytes: 0,
            }),
            has_project_snapshot: false,
        }])
    }

    async fn submit(&self, delivery: CommandDelivery) -> Result<(), BackendError> {
        self.check_location(&delivery.location)?;
        self.executor
            .submit(delivery)
            .await
            .map_err(|e| BackendError::Other(format!("{e:#}")))
    }

    async fn cancel(&self, request: CancelRequest) -> Result<(), BackendError> {
        self.check_location(&request.location)?;
        self.executor
            .cancel(request.key)
            .await
            .map_err(|e| BackendError::Other(format!("{e:#}")))
    }

    async fn reconcile(&self, query: ReconcileQuery) -> Result<ReconcilePage, BackendError> {
        self.check_location(&query.location)?;
        let mut operations = Vec::new();
        for key in query.operations {
            let status = self
                .executor
                .status(key)
                .await
                .map_err(|e| BackendError::Other(format!("{e:#}")))?;
            operations.push((key, status));
        }
        Ok(ReconcilePage {
            operations,
            orbs: Vec::new(),
            next_page_token: None,
        })
    }

    async fn acknowledge_completion(
        &self,
        location: &Location,
        key: CommandKey,
    ) -> Result<(), BackendError> {
        self.check_location(location)?;
        self.executor
            .acknowledge(key)
            .await
            .map_err(|e| BackendError::Other(format!("{e:#}")))
    }
}
