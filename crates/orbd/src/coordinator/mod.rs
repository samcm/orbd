//! Coordinator core: admission, durable request queue, command journal and outbox,
//! result application, recovery, and the local `Dispatch` service.

use crate::config::coordinator::CoordinatorConfig;
use tokio_util::sync::CancellationToken;

/// Runs the coordinator until `shutdown` is cancelled.
pub async fn run(config: CoordinatorConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let _ = (config, shutdown);
    anyhow::bail!("coordinator not implemented yet")
}
