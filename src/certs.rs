//! The test certificate both TLS stacks use.
//!
//! Vendored from `quic/s2n-quic-core/certs` in the s2n-quic repository, where it is published for
//! demonstration purposes. It is a self-signed certificate with `localhost` and `127.0.0.1` in
//! its SANs, which both s2n-tls and rustls accept as a trust anchor. **Never use it for anything
//! but local testing.**

use anyhow::{Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub const CERT_PEM: &str = include_str!("../certs/cert.pem");
pub const KEY_PEM: &str = include_str!("../certs/key.pem");

pub fn chain() -> Result<Vec<CertificateDer<'static>>> {
    rustls_pemfile::certs(&mut CERT_PEM.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parsing the test certificate")
}

pub fn key() -> Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut KEY_PEM.as_bytes())
        .context("parsing the test key")?
        .context("the test key file holds no private key")
}

pub fn root_store() -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in chain()? {
        roots.add(cert).context("trusting the test certificate")?;
    }

    Ok(roots)
}
