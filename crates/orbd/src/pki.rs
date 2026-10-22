//! Built-in PKI for the agent fleet: the coordinator's CA, certificates issued from
//! policy (never from what a CSR asks for), and join tokens.
//!
//! All keys are ECDSA P-256. Agents generate their key locally; it never leaves them.
//! Agent certificates name the agent id (CN and DNS SAN), are client-auth only and last
//! [`LEAF_LIFETIME`]; agents renew them at about two thirds of their life.

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use rand::Rng;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    PKCS_ECDSA_P256_SHA256, PublicKeyData, SanType, SerialNumber,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

pub const LEAF_LIFETIME: Duration = Duration::from_secs(90 * 24 * 3600);
const CA_LIFETIME: Duration = Duration::from_secs(10 * 365 * 24 * 3600);
/// Backdating for clock skew between coordinator and agents.
const BACKDATE: Duration = Duration::from_secs(3600);

/// The host of an `https://` URL (brackets stripped from IPv6 literals).
pub fn url_host(url: &str) -> Result<String, String> {
    let uri: tonic::transport::Uri = url.parse().map_err(|e| format!("{url:?}: {e}"))?;
    if uri.scheme_str() != Some("https") {
        return Err(format!("{url:?} must be an https:// URL"));
    }
    let host = uri.host().ok_or_else(|| format!("{url:?} has no host"))?;
    Ok(host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_owned())
}

/// Agent ids end up in DNS SANs: lowercase letters, digits, `-` and `.`, 1–63 chars.
pub fn validate_agent_id(id: &str) -> Result<(), String> {
    let ok = (1..=63).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id.as_bytes()[id.len() - 1].is_ascii_alphanumeric();
    if ok {
        Ok(())
    } else {
        Err(format!(
            "invalid agent id {:?}: use 1-63 lowercase letters, digits, '-' or '.'",
            id.escape_debug().to_string()
        ))
    }
}

pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Identity of a public key: SHA-256 of its raw subject-public-key bits.
pub fn key_sha256(raw_public_key: &[u8]) -> String {
    sha256_hex(raw_public_key)
}

fn now_plus(d: Duration, sign: i8) -> time::OffsetDateTime {
    let d = time::Duration::try_from(d).expect("duration fits");
    let now = time::OffsetDateTime::now_utc();
    if sign < 0 { now - d } else { now + d }
}

fn random_serial() -> SerialNumber {
    let mut b = [0u8; 16];
    rand::rng().fill_bytes(&mut b);
    b[0] &= 0x7f;
    SerialNumber::from_slice(&b)
}

/// Facts about a DER certificate that orbd cares about.
pub struct CertInfo {
    /// Subject CNs and DNS SANs.
    pub names: Vec<String>,
    pub key_sha256: String,
    pub not_before_ms: u64,
    pub not_after_ms: u64,
}

pub fn cert_info(der: &[u8]) -> Result<CertInfo> {
    use x509_parser::extensions::GeneralName;
    let (_, cert) = x509_parser::parse_x509_certificate(der).context("parsing certificate")?;
    let mut names: Vec<String> = cert
        .subject()
        .iter_common_name()
        .filter_map(|cn| cn.as_str().ok().map(str::to_owned))
        .collect();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let GeneralName::DNSName(d) = n {
                names.push((*d).to_owned());
            }
        }
    }
    let ms = |t: x509_parser::time::ASN1Time| (t.timestamp().max(0) as u64).saturating_mul(1000);
    Ok(CertInfo {
        names,
        key_sha256: key_sha256(&cert.public_key().subject_public_key.data),
        not_before_ms: ms(cert.validity().not_before),
        not_after_ms: ms(cert.validity().not_after),
    })
}

pub fn der_to_pem(tag: &str, der: &[u8]) -> String {
    let b64 = STANDARD.encode(der);
    let mut out = format!("-----BEGIN {tag}-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).expect("base64 is ascii"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {tag}-----\n"));
    out
}

/// Writes `data` to `path` atomically (temp file, fsync, rename, fsync dir), mode 0600.
pub fn write_private(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("creating {}", tmp.display()))?;
    f.write_all(data)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, path).with_context(|| format!("installing {}", path.display()))?;
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

pub fn sync_dir(dir: &Path) -> Result<()> {
    std::fs::File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing {}", dir.display()))
}

/// Creates `dir` (and parents) with mode 0700.
pub fn private_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("chmod {}", dir.display()))
}

// --- coordinator CA ----------------------------------------------------------

pub struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pub cert_der: Vec<u8>,
}

impl Ca {
    /// Loads the CA from `<dir>/ca.{pem,key}`. Creates it only when `first_init` (no
    /// coordinator database yet); otherwise a missing CA is an error, because replacing
    /// it would silently cut off every enrolled agent.
    pub fn load_or_create(dir: &Path, first_init: bool) -> Result<Self> {
        if !dir.exists() {
            if !first_init {
                bail!(
                    "{} is missing but coordinator state exists; refusing to create a new CA \
                     (restore it, or delete the state directory to start over)",
                    dir.display()
                );
            }
            Self::create(dir)?;
        }
        let key_pem = std::fs::read_to_string(dir.join("ca.key"))
            .with_context(|| format!("reading {}/ca.key", dir.display()))?;
        let cert_pem = std::fs::read_to_string(dir.join("ca.pem"))
            .with_context(|| format!("reading {}/ca.pem", dir.display()))?;
        let key = KeyPair::from_pem(&key_pem).context("parsing CA key")?;
        let issuer = Issuer::from_ca_cert_pem(&cert_pem, key).context("parsing CA certificate")?;
        let cert_der = pem_body(&cert_pem)?;
        Ok(Self { issuer, cert_der })
    }

    /// Writes a new CA into a temporary directory and renames it into place, so a crash
    /// never leaves half a CA.
    fn create(dir: &Path) -> Result<()> {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let mut p = CertificateParams::default();
        p.distinguished_name = DistinguishedName::new();
        p.distinguished_name
            .push(DnType::CommonName, "orbd agent CA");
        p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        p.not_before = now_plus(BACKDATE, -1);
        p.not_after = now_plus(CA_LIFETIME, 1);
        p.serial_number = Some(random_serial());
        let cert = p.self_signed(&key)?;
        let tmp = dir.with_extension("tmp");
        let _ = std::fs::remove_dir_all(&tmp);
        private_dir(&tmp)?;
        write_private(&tmp.join("ca.key"), key.serialize_pem().as_bytes())?;
        write_private(&tmp.join("ca.pem"), cert.pem().as_bytes())?;
        std::fs::rename(&tmp, dir).with_context(|| format!("installing {}", dir.display()))?;
        sync_dir(dir.parent().unwrap_or(Path::new(".")))?;
        tracing::info!(dir = %dir.display(), "created agent CA");
        Ok(())
    }

    fn leaf(names: Vec<SanType>, cn: &str, usage: ExtendedKeyUsagePurpose) -> CertificateParams {
        let mut p = CertificateParams::default();
        p.subject_alt_names = names;
        p.distinguished_name = DistinguishedName::new();
        p.distinguished_name.push(DnType::CommonName, cn);
        p.is_ca = IsCa::ExplicitNoCa;
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![usage];
        p.use_authority_key_identifier_extension = true;
        p.not_before = now_plus(BACKDATE, -1);
        p.not_after = now_plus(LEAF_LIFETIME, 1);
        p.serial_number = Some(random_serial());
        p
    }

    /// Issues an agent certificate for the key in `csr_der` (signature verified). The
    /// certificate's names and extensions come from policy only.
    pub fn issue_agent(&self, agent_id: &str, csr_der: &[u8]) -> Result<(Vec<u8>, String)> {
        let csr = parse_csr(csr_der)?;
        let san = SanType::DnsName(agent_id.try_into().map_err(|e| anyhow!("{e:?}"))?);
        let cert = Self::leaf(vec![san], agent_id, ExtendedKeyUsagePurpose::ClientAuth)
            .signed_by(&csr.public_key, &self.issuer)?;
        Ok((cert.der().to_vec(), key_sha256(csr.public_key.der_bytes())))
    }

    /// A fresh server certificate for `host`: DER chain (leaf, CA) and PKCS#8 DER key.
    pub fn issue_server(&self, host: &str) -> Result<(Vec<Vec<u8>>, Vec<u8>)> {
        let san = match host.parse::<std::net::IpAddr>() {
            Ok(ip) => SanType::IpAddress(ip),
            Err(_) => SanType::DnsName(host.try_into().map_err(|e| anyhow!("{e:?}"))?),
        };
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
        let cert = Self::leaf(vec![san], host, ExtendedKeyUsagePurpose::ServerAuth)
            .signed_by(&key, &self.issuer)?;
        Ok((
            vec![cert.der().to_vec(), self.cert_der.clone()],
            key.serialize_der(),
        ))
    }
}

/// Parses a CSR, checks its signature and requires a P-256 key.
fn parse_csr(der: &[u8]) -> Result<CertificateSigningRequestParams> {
    let csr = CertificateSigningRequestParams::from_der(&der.to_vec().into())
        .map_err(|e| anyhow!("invalid CSR: {e}"))?;
    if csr.public_key.algorithm() != &PKCS_ECDSA_P256_SHA256 {
        bail!("CSR key must be ECDSA P-256");
    }
    Ok(csr)
}

/// Key identity of a CSR (signature verified).
pub fn csr_key_sha256(der: &[u8]) -> Result<String> {
    Ok(key_sha256(parse_csr(der)?.public_key.der_bytes()))
}

/// The DER body of the first PEM block in `pem`.
pub fn pem_body(pem: &str) -> Result<Vec<u8>> {
    let body: String = pem
        .lines()
        .skip_while(|l| !l.starts_with("-----BEGIN"))
        .skip(1)
        .take_while(|l| !l.starts_with("-----END"))
        .collect();
    STANDARD.decode(body).context("decoding PEM")
}

// --- agent keys ----------------------------------------------------------------

/// A new agent key (PKCS#8 PEM) and a CSR for it.
pub fn new_agent_key(agent_id: &str) -> Result<(String, Vec<u8>)> {
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256)?;
    let csr = csr_for(&key, agent_id)?;
    Ok((key.serialize_pem(), csr))
}

/// A CSR for an existing agent key (renewal reuses the key).
pub fn agent_csr(key_pem: &str, agent_id: &str) -> Result<Vec<u8>> {
    csr_for(&KeyPair::from_pem(key_pem)?, agent_id)
}

fn csr_for(key: &KeyPair, agent_id: &str) -> Result<Vec<u8>> {
    let mut p = CertificateParams::default();
    p.distinguished_name = DistinguishedName::new();
    p.distinguished_name.push(DnType::CommonName, agent_id);
    Ok(p.serialize_request(key)?.der().to_vec())
}

// --- join tokens -----------------------------------------------------------------

const TOKEN_VERSION: u32 = 1;

/// Everything an agent needs to enroll: where to dial, which CA to trust, and a
/// single-use credential. Opaque to users (base64url JSON); never logged.
pub struct JoinToken {
    pub coordinator_url: String,
    pub ca_der: Vec<u8>,
    pub agent_id: String,
    pub token_id: String,
    pub secret: [u8; 32],
}

impl std::fmt::Debug for JoinToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinToken({}, redacted)", self.agent_id)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenWire {
    v: u32,
    url: String,
    ca: String,
    ca_sha256: String,
    agent: String,
    id: String,
    secret: String,
}

impl JoinToken {
    pub fn new(coordinator_url: &str, ca_der: &[u8], agent_id: &str) -> Self {
        let mut id = [0u8; 16];
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut id);
        rand::rng().fill_bytes(&mut secret);
        Self {
            coordinator_url: coordinator_url.to_owned(),
            ca_der: ca_der.to_vec(),
            agent_id: agent_id.to_owned(),
            token_id: hex::encode(id),
            secret,
        }
    }

    pub fn encode(&self) -> String {
        let wire = TokenWire {
            v: TOKEN_VERSION,
            url: self.coordinator_url.clone(),
            ca: URL_SAFE_NO_PAD.encode(&self.ca_der),
            ca_sha256: sha256_hex(&self.ca_der),
            agent: self.agent_id.clone(),
            id: self.token_id.clone(),
            secret: URL_SAFE_NO_PAD.encode(self.secret),
        };
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&wire).expect("token serializes"))
    }

    /// Decodes and checks a token, including that the CA matches its fingerprint.
    pub fn decode(token: &str) -> Result<Self> {
        let bad = || anyhow!("malformed join token");
        let json = URL_SAFE_NO_PAD.decode(token.trim()).map_err(|_| bad())?;
        let w: TokenWire = serde_json::from_slice(&json).map_err(|_| bad())?;
        if w.v != TOKEN_VERSION {
            bail!("join token version {} is not supported", w.v);
        }
        let ca_der = URL_SAFE_NO_PAD.decode(&w.ca).map_err(|_| bad())?;
        if sha256_hex(&ca_der) != w.ca_sha256 {
            bail!("join token CA does not match its fingerprint");
        }
        let secret: [u8; 32] = URL_SAFE_NO_PAD
            .decode(&w.secret)
            .ok()
            .and_then(|s| s.try_into().ok())
            .ok_or_else(bad)?;
        url_host(&w.url).map_err(|e| anyhow!("join token: {e}"))?;
        validate_agent_id(&w.agent).map_err(|e| anyhow!("join token: {e}"))?;
        Ok(Self {
            coordinator_url: w.url,
            ca_der,
            agent_id: w.agent,
            token_id: w.id,
            secret,
        })
    }
}
