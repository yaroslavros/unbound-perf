//! The framed CONNECT measurement, run against the unmodified upstream libraries.
//!
//! This binary exists to answer one question: do the forked libraries slow down plain framed
//! CONNECT? It compiles the *same* harness source as `unbound-perf` — the modules below are the
//! files in `../src`, not copies — but against upstream h3, h3-quinn, s2n-quic, s2n-quic-h3,
//! quiche and tokio-quiche pinned to the commits each fork branched from. The `unbound` feature
//! is off, so only the framed mode is built and the unbound data API is never referenced.
//!
//! Compare its `framed` column against the `framed` column of `unbound-perf`.

#[path = "../../src/certs.rs"]
mod certs;
#[path = "../../src/quiche_backend.rs"]
mod quiche_backend;
#[path = "../../src/quinn_backend.rs"]
mod quinn_backend;
#[path = "../../src/runner.rs"]
mod runner;
#[path = "../../src/s2n_backend.rs"]
mod s2n_backend;
#[path = "../../src/tunnel.rs"]
mod tunnel;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    runner::run().await
}
