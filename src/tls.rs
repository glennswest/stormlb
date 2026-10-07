//! TLS termination for the router (`[router.tls]`, #14).
//!
//! The router serves whatever certificate/key pairs it is given — on a node,
//! the wildcard `*.storm1.<zone>` that stormcert mints — and picks one per
//! handshake by SNI: the first pair whose certificate is valid for the name
//! (webpki's own check, so a wildcard matches exactly one label), else the
//! first pair. Files are re-read when they change, so a renewal is picked up
//! without a restart; a pair that can't be read or parsed keeps its last good
//! version and says so, and one never loaded is skipped.
//!
//! After the handshake the connection is plain HTTP/1.1 to the router's
//! demux, exactly as on `:80`. ALPN offers only `http/1.1`.

use serde::Deserialize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio_rustls::rustls::crypto::ring::sign::any_supported_type;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::ServerConfig;
use tracing::{info, warn};

/// `[router.tls]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsCfg {
    /// Where the TLS listener binds; `auto:<port>` as for `[router] listen`.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// The certificate/key pairs, PEM. The first is the default for a client
    /// that sends no SNI, or a name none of them covers.
    #[serde(default)]
    pub certs: Vec<CertPair>,
    /// Once a certificate is loaded, answer plain-HTTP requests with a 308 to
    /// https (all but `/healthz`).
    #[serde(default = "default_redirect")]
    pub redirect: bool,
    /// How often the files are checked for a change (seconds, min 1).
    #[serde(default = "default_reload")]
    pub reload_secs: u64,
}
fn default_listen() -> String {
    "auto:443".into()
}
fn default_redirect() -> bool {
    true
}
fn default_reload() -> u64 {
    30
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CertPair {
    /// The certificate chain, leaf first.
    pub cert_file: String,
    pub key_file: String,
}

struct Loaded {
    leaf: CertificateDer<'static>,
    key: Arc<CertifiedKey>,
    stamp: (Option<SystemTime>, Option<SystemTime>),
}

/// The pairs, as last loaded.
pub struct Certs {
    pairs: Vec<CertPair>,
    loaded: RwLock<Vec<Option<Loaded>>>,
    /// Each pair's last load error, so a missing file is logged once, not
    /// every `reload_secs`.
    errors: RwLock<Vec<Option<String>>>,
}

impl std::fmt::Debug for Certs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Certs").field("pairs", &self.pairs.len()).field("loaded", &self.count()).finish()
    }
}

impl Certs {
    /// Load every pair now (see [`Certs::reload`]).
    pub fn new(pairs: Vec<CertPair>) -> Arc<Certs> {
        let n = pairs.len();
        let c = Arc::new(Certs {
            pairs,
            loaded: RwLock::new((0..n).map(|_| None).collect()),
            errors: RwLock::new(vec![None; n]),
        });
        c.reload();
        c
    }

    /// Re-read each pair whose files changed (or that never loaded). A pair
    /// that fails keeps its last good version. Returns how many are loaded.
    pub fn reload(&self) -> usize {
        for (i, p) in self.pairs.iter().enumerate() {
            let stamp = (mtime(&p.cert_file), mtime(&p.key_file));
            let current = self.loaded.read().unwrap()[i].as_ref().map(|l| l.stamp);
            if current == Some(stamp) {
                continue;
            }
            match load(p) {
                Ok((leaf, key)) => {
                    info!("router TLS: loaded {}", p.cert_file);
                    self.loaded.write().unwrap()[i] = Some(Loaded { leaf, key, stamp });
                    self.errors.write().unwrap()[i] = None;
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    let mut errors = self.errors.write().unwrap();
                    if errors[i].as_deref() != Some(msg.as_str()) {
                        if current.is_some() {
                            warn!("router TLS: {} changed but does not load, keeping the last good one: {msg}", p.cert_file);
                        } else {
                            warn!("router TLS: {}: {msg} (checked again every reload; logged once)", p.cert_file);
                        }
                        errors[i] = Some(msg);
                    }
                }
            }
        }
        self.count()
    }

    /// How many pairs are loaded.
    pub fn count(&self) -> usize {
        self.loaded.read().unwrap().iter().flatten().count()
    }

    /// The pair for `sni`: the first whose certificate is valid for it,
    /// else the first loaded.
    pub fn pick(&self, sni: Option<&str>) -> Option<Arc<CertifiedKey>> {
        let loaded = self.loaded.read().unwrap();
        let name = sni.and_then(|s| ServerName::try_from(s.to_string()).ok());
        if let Some(name) = &name {
            for l in loaded.iter().flatten() {
                let ok = webpki::EndEntityCert::try_from(&l.leaf)
                    .map(|c| c.verify_is_valid_for_subject_name(name).is_ok())
                    .unwrap_or(false);
                if ok {
                    return Some(l.key.clone());
                }
            }
        }
        loaded.iter().flatten().next().map(|l| l.key.clone())
    }

    /// Re-check the files every `secs`, forever.
    pub async fn reload_every(self: Arc<Self>, secs: u64) {
        loop {
            tokio::time::sleep(Duration::from_secs(secs.max(1))).await;
            let c = self.clone();
            let _ = tokio::task::spawn_blocking(move || c.reload()).await;
        }
    }
}

impl ResolvesServerCert for Certs {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.pick(hello.server_name())
    }
}

/// The server config: these certificates, HTTP/1.1 only.
pub fn server_config(certs: Arc<Certs>) -> Arc<ServerConfig> {
    let mut cfg = ServerConfig::builder().with_no_client_auth().with_cert_resolver(certs);
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(cfg)
}

fn mtime(path: &str) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

fn load(p: &CertPair) -> anyhow::Result<(CertificateDer<'static>, Arc<CertifiedKey>)> {
    use anyhow::Context;
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&p.cert_file)
        .with_context(|| format!("reading {}", p.cert_file))?
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing {}", p.cert_file))?;
    let leaf = chain.first().cloned().with_context(|| format!("{}: no certificate in it", p.cert_file))?;
    let key = PrivateKeyDer::from_pem_file(&p.key_file).with_context(|| format!("reading the key {}", p.key_file))?;
    let signer = any_supported_type(&key).map_err(|e| anyhow::anyhow!("{}: {e}", p.key_file))?;
    let ck = CertifiedKey::new(chain, signer);
    ck.keys_match().map_err(|e| anyhow::anyhow!("{} does not belong to {}: {e}", p.key_file, p.cert_file))?;
    webpki::EndEntityCert::try_from(&leaf).map_err(|e| anyhow::anyhow!("{}: {e}", p.cert_file))?;
    Ok((leaf, Arc::new(ck)))
}
