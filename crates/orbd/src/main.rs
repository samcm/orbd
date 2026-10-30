use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

const DEFAULT_CONFIG: &str = "/etc/orbd/orbd.yaml";

#[derive(Parser)]
#[command(name = "orbd", version, about = "Self-hosted backend for Amp orbs")]
struct Cli {
    /// Log as JSON lines.
    #[arg(long, global = true, env = "ORBD_LOG_JSON")]
    log_json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct CoordinatorArgs {
    /// Coordinator configuration.
    #[arg(long, short, env = "ORBD_CONFIG", default_value = DEFAULT_CONFIG)]
    config: PathBuf,
}

#[derive(Subcommand)]
enum Command {
    /// Run the coordinator (wraps one `amp orb provider` per offering).
    Coordinator(CoordinatorArgs),
    /// Run an agent on a compute host (or manage agents: `orbd agent list|remove`).
    #[command(args_conflicts_with_subcommands = true)]
    Agent {
        #[command(subcommand)]
        manage: Option<AgentCommand>,
        #[command(flatten)]
        run: AgentArgs,
    },
    /// Print a single-use join token for a new agent (run on the coordinator).
    JoinToken {
        /// Id for the new agent, e.g. `host-1`.
        agent_id: String,
        /// Validity in seconds.
        #[arg(long, default_value_t = 3600)]
        ttl_secs: u64,
        #[command(flatten)]
        coordinator: CoordinatorArgs,
    },
    /// List or delete orbs (run on the coordinator).
    #[command(subcommand)]
    Orb(OrbCommand),
    /// Handle one Amp dispatch job from the environment (run by `amp orb provider`).
    Dispatch,
    /// Check host prerequisites (QEMU, /dev/kvm, guest kernel, umoci, mkfs.ext4).
    Doctor {
        /// Agent config to take tool and asset paths from.
        #[arg(long, short, env = "ORBD_AGENT_CONFIG")]
        config: Option<PathBuf>,
        /// Guest kernel (bzImage) to check [default: runtime.qemu.kernel, else
        /// /usr/share/orbd/bzImage].
        #[arg(long)]
        kernel: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AgentCommand {
    /// List enrolled agents (run on the coordinator).
    List(CoordinatorArgs),
    /// Revoke an agent for good; its orbs stay recorded on it, unavailable.
    Remove {
        agent_id: String,
        #[command(flatten)]
        coordinator: CoordinatorArgs,
    },
}

#[derive(Subcommand)]
enum OrbCommand {
    /// List orbs: Amp orb id, state and for how long, agent, offering.
    List(CoordinatorArgs),
    /// Delete an orb whose thread is gone (Amp sends no delete for an archived thread),
    /// as if Amp had sent `delete`. Refused unless the orb was last seen asleep or
    /// stopped at least `--min-asleep-mins` ago, with no job from Amp open or started
    /// since (the coordinator keeps finished jobs for about an hour). A later spawn
    /// starts a fresh orb.
    Delete {
        /// Amp orb id (Amp's ORB_ID, `amp-<thread id>`), as `orbd orb list` shows it.
        amp_orb_id: String,
        /// How long ago the orb must have been last seen asleep or stopped.
        #[arg(long, default_value_t = 60)]
        min_asleep_mins: u64,
        #[command(flatten)]
        coordinator: CoordinatorArgs,
    },
}

#[derive(Args)]
struct AgentArgs {
    /// Join token from `orbd join-token`; needed only for the first start.
    #[arg(long, env = "ORBD_JOIN", hide_env_values = true)]
    join: Option<String>,
    #[arg(long, env = "ORBD_STATE_DIR", default_value = "/var/lib/orbd")]
    state_dir: PathBuf,
    /// DNS servers for orbs, comma-separated.
    #[arg(long, env = "ORBD_DNS", value_delimiter = ',', default_values_t = orbd::config::agent::DEFAULT_DNS.map(String::from))]
    dns: Vec<String>,
    /// Overrides the detected vCPU ceiling.
    #[arg(long, env = "ORBD_CAPACITY_VCPUS")]
    capacity_vcpus: Option<u32>,
    /// Overrides the detected memory ceiling.
    #[arg(long, env = "ORBD_CAPACITY_MEMORY_MIB")]
    capacity_memory_mib: Option<u64>,
    /// Limits what orbs may allocate on disk [default: the filesystem's size].
    #[arg(long, env = "ORBD_CAPACITY_DISK_GIB")]
    capacity_disk_gib: Option<u64>,
    /// Free disk to keep: no orb starts or wakes below it [default: max(10 GiB, 5%)].
    #[arg(long, env = "ORBD_DISK_RESERVE_GIB")]
    disk_reserve_gib: Option<u64>,
    /// Memory to keep available on the host: no orb starts or wakes unless the host's
    /// available memory, less this, holds it [default: max(4 GiB, 10% of memory)].
    #[arg(long, env = "ORBD_MEMORY_RESERVE_GIB")]
    memory_reserve_gib: Option<u64>,
    /// Prometheus listener, e.g. `127.0.0.1:9465`.
    #[arg(long, env = "ORBD_METRICS_LISTEN")]
    metrics_listen: Option<std::net::SocketAddr>,
    /// Optional YAML with advanced runtime and image settings.
    #[arg(long, short, env = "ORBD_AGENT_CONFIG")]
    config: Option<PathBuf>,
}

fn admin_socket(args: &CoordinatorArgs) -> anyhow::Result<PathBuf> {
    Ok(orbd::config::coordinator::CoordinatorConfig::load(&args.config)?.admin_socket())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    orbd::configure_failpoints();
    let rt = tokio::runtime::Runtime::new()?;
    use orbd::backend::fleet::admin;
    match cli.command {
        Command::Dispatch => {
            // Logs from the coordinator are forwarded verbatim; keep our own quiet.
            let code = rt.block_on(orbd::amp::dispatch::run_from_env());
            std::process::exit(code);
        }
        Command::Doctor { config, kernel } => {
            let ok = orbd::doctor::run(&orbd::doctor::DoctorOptions { config, kernel })?;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Command::JoinToken {
            agent_id,
            ttl_secs,
            coordinator,
        } => {
            let socket = admin_socket(&coordinator)?;
            let ttl = std::time::Duration::from_secs(ttl_secs);
            println!(
                "{}",
                rt.block_on(admin::cli_join_token(&socket, &agent_id, ttl))?
            );
            Ok(())
        }
        Command::Agent {
            manage: Some(AgentCommand::List(coordinator)),
            ..
        } => {
            print!(
                "{}",
                rt.block_on(admin::cli_list(&admin_socket(&coordinator)?))?
            );
            Ok(())
        }
        Command::Agent {
            manage:
                Some(AgentCommand::Remove {
                    agent_id,
                    coordinator,
                }),
            ..
        } => rt.block_on(admin::cli_remove(&admin_socket(&coordinator)?, &agent_id)),
        Command::Orb(OrbCommand::List(coordinator)) => {
            print!(
                "{}",
                rt.block_on(orbd::coordinator::orbs::cli_list(&admin_socket(
                    &coordinator
                )?))?
            );
            Ok(())
        }
        Command::Orb(OrbCommand::Delete {
            amp_orb_id,
            min_asleep_mins,
            coordinator,
        }) => {
            let min_asleep = std::time::Duration::from_secs(min_asleep_mins.saturating_mul(60));
            println!(
                "{}",
                rt.block_on(orbd::coordinator::orbs::cli_delete(
                    &admin_socket(&coordinator)?,
                    &amp_orb_id,
                    min_asleep
                ))?
            );
            Ok(())
        }
        Command::Coordinator(args) => {
            orbd::telemetry::init_logging(cli.log_json);
            let config = orbd::config::coordinator::CoordinatorConfig::load(&args.config)?;
            rt.block_on(async {
                let shutdown = shutdown_on_signal();
                orbd::coordinator::run(config, shutdown).await
            })
        }
        Command::Agent {
            manage: None,
            run: a,
        } => {
            orbd::telemetry::init_logging(cli.log_json);
            let mut config = match &a.config {
                Some(p) => orbd::config::agent::AgentConfig::load(p)?,
                None => Default::default(),
            };
            let mut s = orbd::config::agent::AgentSection::new(a.state_dir);
            s.dns_servers = a.dns;
            s.capacity = orbd::config::agent::CapacityOverrides {
                vcpus: a.capacity_vcpus,
                memory_mib: a.capacity_memory_mib,
                disk_gib: a.capacity_disk_gib,
                disk_reserve_gib: a.disk_reserve_gib,
                memory_reserve_gib: a.memory_reserve_gib,
            };
            s.metrics_listen = a.metrics_listen;
            config.agent = s;
            rt.block_on(async {
                let shutdown = shutdown_on_signal();
                orbd::agent::run(config, a.join, shutdown).await
            })
        }
    }
}

fn shutdown_on_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
        tracing::info!("shutdown requested");
        t.cancel();
    });
    token
}
