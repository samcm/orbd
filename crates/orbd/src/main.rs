use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "orbd", version, about = "Self-hosted backend for Amp orbs")]
struct Cli {
    /// Log as JSON lines.
    #[arg(long, global = true, env = "ORBD_LOG_JSON")]
    log_json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the coordinator (wraps one `amp orb provider` per offering).
    Coordinator {
        #[arg(long, short, env = "ORBD_CONFIG")]
        config: PathBuf,
    },
    /// Run an agent on a compute host.
    Agent {
        #[arg(long, short, env = "ORBD_CONFIG")]
        config: PathBuf,
    },
    /// Handle one Amp dispatch job from the environment (run by `amp orb provider`).
    Dispatch,
    /// Print a new 32-byte secret key (hex) for `secret_key_file`.
    GenKey,
    /// Check host prerequisites (QEMU, /dev/kvm, guest kernel, umoci, mkfs.ext4).
    Doctor {
        /// Agent config to take tool and asset paths from.
        #[arg(long, short, env = "ORBD_CONFIG")]
        config: Option<PathBuf>,
        /// Guest kernel (bzImage) to check.
        #[arg(long)]
        kernel: Option<PathBuf>,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    orbd::configure_failpoints();
    let rt = tokio::runtime::Runtime::new()?;
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
        Command::GenKey => {
            print!("{}", orbd::secrets::SecretKey::generate_hex());
            Ok(())
        }
        Command::Coordinator { config } => {
            orbd::telemetry::init_logging(cli.log_json);
            let config = orbd::config::coordinator::CoordinatorConfig::load(&config)?;
            rt.block_on(async {
                let shutdown = shutdown_on_signal();
                orbd::coordinator::run(config, shutdown).await
            })
        }
        Command::Agent { config } => {
            orbd::telemetry::init_logging(cli.log_json);
            let config = orbd::config::agent::AgentConfig::load(&config)?;
            rt.block_on(async {
                let shutdown = shutdown_on_signal();
                orbd::agent::run(config, shutdown).await
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
