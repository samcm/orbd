//! Agent-local network attachment for one VM.
//!
//! The backend is chosen when the orb's disk is created and persisted in its recipe;
//! a sleeping VM is always restored with the backend (and MAC) it was saved with.
//! Only QEMU user-mode networking (slirp) exists for now; TAP in a per-orb network
//! namespace with NAT replaces it in production and slots in as another variant with
//! real `prepare`/`adopt`/`remove` work.

use orbd_proto::guest::v1 as gpb;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::model::OrbId;

/// Typed backend; serialized into the VM recipe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum NetBackend {
    /// QEMU built-in slirp: no host privileges, guest-initiated traffic only.
    User,
}

/// Durable per-orb attachment metadata (stable across sleep).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetAttachment {
    pub backend: NetBackend,
    pub mac: String,
    pub ipv4_cidr: String,
    pub ipv4_gateway: String,
    pub mtu: u32,
}

/// slirp's fixed private network (each VM has its own).
const USER_NET: &str = "10.0.2.0/24";
const USER_HOST: &str = "10.0.2.2";
const USER_GUEST: &str = "10.0.2.15";

impl NetAttachment {
    /// Allocates the attachment for a new orb (before its recipe is published).
    pub fn prepare(orb: OrbId) -> Self {
        Self {
            backend: NetBackend::User,
            mac: stable_mac(orb),
            ipv4_cidr: format!("{USER_GUEST}/24"),
            ipv4_gateway: USER_HOST.into(),
            mtu: 1500,
        }
    }

    /// Checks host-side resources still exist before a (re)launch. Nothing to check
    /// for slirp, which lives inside the QEMU process.
    pub fn adopt(&self) -> Result<(), String> {
        match self.backend {
            NetBackend::User => Ok(()),
        }
    }

    /// Releases host-side resources. Idempotent.
    pub fn remove(&self) {
        match self.backend {
            NetBackend::User => {}
        }
    }

    /// Guest-visible QEMU arguments (part of the device topology recorded in manifests).
    pub fn qemu_args(&self) -> Vec<String> {
        match self.backend {
            NetBackend::User => vec![
                "-netdev".into(),
                format!(
                    "user,id=net0,ipv6=off,net={USER_NET},host={USER_HOST},dhcpstart={USER_GUEST}"
                ),
                "-device".into(),
                format!("virtio-net-pci,id=net0dev,netdev=net0,mac={}", self.mac),
            ],
        }
    }

    /// Static addressing for `Wake.network`; DNS is explicit config, never the host's
    /// resolv.conf (slirp's own resolver forwards to it).
    pub fn guest_config(&self, dns_servers: &[String]) -> gpb::NetworkConfig {
        gpb::NetworkConfig {
            dns_servers: dns_servers.to_vec(),
            search_domains: Vec::new(),
            mac: self.mac.clone(),
            ipv4_cidr: self.ipv4_cidr.clone(),
            ipv4_gateway: self.ipv4_gateway.clone(),
            mtu: self.mtu,
        }
    }
}

/// QEMU's OUI plus three bytes derived from the orb id.
fn stable_mac(orb: OrbId) -> String {
    let h = Sha256::digest(orb.0.as_bytes());
    format!("52:54:00:{:02x}:{:02x}:{:02x}", h[0], h[1], h[2])
}
