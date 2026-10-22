//! Agent: dials the coordinator, durably accepts commands, runs one executor per orb
//! against the local runtimes, and replays terminal results until acknowledged.

use crate::config::agent::AgentConfig;
use tokio_util::sync::CancellationToken;

/// Runs the agent until `shutdown` is cancelled. Running orbs survive agent exit.
pub async fn run(config: AgentConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let _ = (config, shutdown);
    anyhow::bail!("agent not implemented yet")
}
