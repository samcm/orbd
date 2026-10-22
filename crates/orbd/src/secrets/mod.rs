//! Secret handling: guest environment values, sealed (encrypted) persistence for crash
//! recovery, and keyed fingerprints for recognising identical spawn retries.
//!
//! Journals store a fingerprint and a reference to a sealed payload, never raw tokens.
//! Losing the key or payload must fail a command explicitly (SECRET_UNAVAILABLE), never
//! bootstrap with stale credentials.

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use rand::RngCore;
use secrecy::{ExposeSecret, SecretBox, SecretString};
use sha2::{Digest, Sha256};
use std::path::Path;
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret key file {path}: {source}")]
    KeyFile {
        path: String,
        source: std::io::Error,
    },
    #[error("secret key file {0} must contain 64 hex characters (32 bytes)")]
    KeyFormat(String),
    #[error("sealed payload could not be opened (wrong key or corrupt)")]
    Open,
    #[error("sealed payload is malformed: {0}")]
    Malformed(String),
}

/// 32-byte deployment key from a protected file. Subkeys are derived per purpose.
pub struct SecretKey {
    seal: SecretBox<[u8; 32]>,
    fingerprint: SecretBox<[u8; 32]>,
}

impl SecretKey {
    /// Reads 64 hex characters (surrounding whitespace ignored).
    pub fn load(path: &Path) -> Result<Self, SecretError> {
        let raw = Zeroizing::new(std::fs::read_to_string(path).map_err(|source| {
            SecretError::KeyFile {
                path: path.display().to_string(),
                source,
            }
        })?);
        let mut bytes = Zeroizing::new([0u8; 32]);
        hex::decode_to_slice(raw.trim(), &mut bytes[..])
            .map_err(|_| SecretError::KeyFormat(path.display().to_string()))?;
        Ok(Self::from_bytes(&bytes))
    }

    pub fn from_bytes(master: &[u8; 32]) -> Self {
        let derive = |label: &[u8]| {
            let mut h = Sha256::new();
            h.update(b"orbd-secret-key-v1:");
            h.update(label);
            h.update(master);
            let out: [u8; 32] = h.finalize().into();
            SecretBox::new(Box::new(out))
        };
        Self {
            seal: derive(b"seal"),
            fingerprint: derive(b"fingerprint"),
        }
    }

    /// Generates a fresh key file body (64 hex chars + newline).
    pub fn generate_hex() -> String {
        let mut b = Zeroizing::new([0u8; 32]);
        rand::rngs::OsRng.fill_bytes(&mut b[..]);
        format!("{}\n", hex::encode(&b[..]))
    }

    /// Encrypts `plaintext`; output is `nonce(24) || ciphertext`. `aad` binds the
    /// payload to its owner (e.g. operation id) so payloads cannot be swapped.
    pub fn seal(&self, plaintext: &[u8], aad: &[u8]) -> Vec<u8> {
        let cipher = XChaCha20Poly1305::new(self.seal.expose_secret().into());
        let mut nonce = [0u8; 24];
        rand::rngs::OsRng.fill_bytes(&mut nonce);
        let ct = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                chacha20poly1305::aead::Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .expect("xchacha20poly1305 encryption cannot fail");
        let mut out = nonce.to_vec();
        out.extend_from_slice(&ct);
        out
    }

    pub fn open(&self, sealed: &[u8], aad: &[u8]) -> Result<Zeroizing<Vec<u8>>, SecretError> {
        if sealed.len() < 24 {
            return Err(SecretError::Malformed("too short".into()));
        }
        let (nonce, ct) = sealed.split_at(24);
        let cipher = XChaCha20Poly1305::new(self.seal.expose_secret().into());
        cipher
            .decrypt(
                XNonce::from_slice(nonce),
                chacha20poly1305::aead::Payload { msg: ct, aad },
            )
            .map(Zeroizing::new)
            .map_err(|_| SecretError::Open)
    }

    /// Keyed fingerprint (hex HMAC-SHA256) over length-prefixed parts.
    pub fn fingerprint(&self, parts: &[&[u8]]) -> String {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(self.fingerprint.expose_secret())
            .expect("hmac accepts any key length");
        for p in parts {
            mac.update(&(p.len() as u64).to_be_bytes());
            mac.update(p);
        }
        hex::encode(mac.finalize().into_bytes())
    }
}

/// Environment variables permitted into a guest (names from ORB_ENVS). Values are secret.
#[derive(Default)]
pub struct PermittedGuestEnv(pub Vec<(String, SecretString)>);

impl std::fmt::Debug for PermittedGuestEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.0.iter().map(|(n, _)| n.as_str()).collect();
        write!(f, "PermittedGuestEnv({names:?}, values redacted)")
    }
}

impl Clone for PermittedGuestEnv {
    fn clone(&self) -> Self {
        Self(
            self.0
                .iter()
                .map(|(n, v)| (n.clone(), SecretString::from(v.expose_secret().to_owned())))
                .collect(),
        )
    }
}

/// Limits for untrusted guest env from Amp.
pub const MAX_GUEST_ENV_VARS: usize = 32;
pub const MAX_GUEST_ENV_NAME: usize = 64;
pub const MAX_GUEST_ENV_VALUE: usize = 8 * 1024;

impl PermittedGuestEnv {
    /// Validates names (`[A-Z_][A-Z0-9_]*`) and sizes.
    pub fn validate(&self) -> Result<(), String> {
        if self.0.len() > MAX_GUEST_ENV_VARS {
            return Err(format!("too many guest env vars ({})", self.0.len()));
        }
        for (name, value) in &self.0 {
            let ok_name = !name.is_empty()
                && name.len() <= MAX_GUEST_ENV_NAME
                && name.bytes().enumerate().all(|(i, b)| {
                    b == b'_' || b.is_ascii_uppercase() || (i > 0 && b.is_ascii_digit())
                });
            if !ok_name {
                return Err(format!("invalid guest env name {name:?}"));
            }
            if value.expose_secret().len() > MAX_GUEST_ENV_VALUE {
                return Err(format!(
                    "guest env {name} exceeds {MAX_GUEST_ENV_VALUE} bytes"
                ));
            }
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&SecretString> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub fn to_json(&self) -> Zeroizing<Vec<u8>> {
        let plain: Vec<(&str, &str)> = self
            .0
            .iter()
            .map(|(n, v)| (n.as_str(), v.expose_secret()))
            .collect();
        Zeroizing::new(serde_json::to_vec(&plain).expect("env serializes"))
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, SecretError> {
        let plain: Vec<(String, String)> =
            serde_json::from_slice(bytes).map_err(|e| SecretError::Malformed(e.to_string()))?;
        Ok(Self(
            plain
                .into_iter()
                .map(|(n, v)| (n, SecretString::from(v)))
                .collect(),
        ))
    }
}
