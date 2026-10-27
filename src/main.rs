//! Throughput of an HTTP/3 CONNECT tunnel, with and without unbound data.
//!
//! Runs a client and a server over loopback and pushes a fixed payload through a CONNECT tunnel
//! three ways — framed in `DATA` frames, unbound but still through h3, and unbound with the
//! stream handed back to the QUIC implementation — for quinn and for s2n-quic.
//!
//! `unbound-perf-baseline` in `baseline/` builds these same modules against the unmodified
//! upstream libraries to measure the framed mode without any of the fork's changes in the path.
//!
//! See the README for what the numbers do and do not mean.

mod certs;
mod quinn_backend;
mod runner;
mod s2n_backend;
mod tunnel;

// Single threaded on purpose: client and server share one thread, so a run measures the total
// protocol work per byte rather than how well two cores overlap. It also keeps the numbers
// stable from run to run.
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    runner::run().await
}
