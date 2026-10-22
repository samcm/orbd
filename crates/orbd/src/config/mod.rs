//! TOML configuration. Everything here is deployment input; the app ships no
//! deployment-specific defaults (no hosts, tokens, offerings or budgets).
//! Unknown fields are rejected. Units are normalised to exact integer bytes at load.

pub mod agent;
pub mod coordinator;

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: String,
        source: Box<toml::de::Error>,
    },
    #[error("invalid configuration: {0}")]
    Invalid(String),
}

pub(crate) fn load_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    toml::from_str(&text).map_err(|source| ConfigError::Parse {
        path: path.display().to_string(),
        source: Box::new(source),
    })
}

/// Human-sized resources in config files.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourcesConfig {
    pub vcpus: u32,
    pub memory_mib: u64,
    pub disk_gib: u64,
}

impl ResourcesConfig {
    pub fn to_resources(self) -> Result<crate::model::Resources, ConfigError> {
        use crate::model::{GIB, MIB};
        let bad = || ConfigError::Invalid("resource size overflows".into());
        Ok(crate::model::Resources {
            vcpus: self.vcpus,
            memory_bytes: self.memory_mib.checked_mul(MIB).ok_or_else(bad)?,
            disk_bytes: self.disk_gib.checked_mul(GIB).ok_or_else(bad)?,
        })
    }
}

/// Files for mutual TLS.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsFiles {
    pub certificate_file: std::path::PathBuf,
    pub private_key_file: std::path::PathBuf,
    /// CA that signs the peer's certificate.
    pub ca_file: std::path::PathBuf,
}
