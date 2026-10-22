//! Logging and Prometheus metrics setup. Metrics never carry per-orb labels.

use std::net::SocketAddr;

/// Initialises `tracing` to stderr. `RUST_LOG` overrides the default `info` filter.
pub fn init_logging(json: bool) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if json {
        let _ = builder.json().try_init();
    } else {
        let _ = builder.try_init();
    }
}

/// Serves Prometheus metrics on `listen` at `/metrics`.
pub fn init_metrics(listen: SocketAddr) -> anyhow::Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(listen)
        .install()?;
    Ok(())
}
