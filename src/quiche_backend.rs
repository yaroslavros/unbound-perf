//! quiche endpoints, driven through tokio-quiche.
//!
//! quiche's HTTP/3 is not generic over a transport the way h3 is, so this backend cannot share
//! `tunnel.rs`. It keeps the same shape instead: one connection per series, all modes interleaved
//! round-robin, the clock starting after the CONNECT response and stopping when the far end has
//! counted the last byte.
//!
//! The handoff mode means something slightly different here. The driver owns the
//! `quiche::Connection`, so an application cannot hold the stream itself;
//! `OutboundFrame::UnboundData { take_over: true }` instead tells the driver to stop running the
//! tunnel through HTTP/3 and to move the bytes straight on the transport. The application keeps
//! using the same channels either way, so the delta is the cost of HTTP/3 itself, not of a
//! different API.

use std::fs;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use quiche::h3::NameValue as _;
use tokio::sync::{mpsc, oneshot};
use tokio_quiche::http3::driver::{
    ClientH3Controller, ClientH3Driver, ClientH3Event, H3Event, InboundFrame, InboundFrameStream,
    NewClientRequest, OutboundFrame, OutboundFrameSender, ServerH3Controller, ServerH3Driver,
    ServerH3Event,
};
use tokio_quiche::http3::settings::Http3Settings;
use tokio_quiche::metrics::DefaultMetrics;
use tokio_quiche::quic::connect_with_config;
use tokio_quiche::settings::{
    CertificateKind, ConnectionParams, Hooks, QuicSettings, TlsCertificatePaths,
};
use tokio_quiche::socket::Socket;

use crate::tunnel::{Mode, RunConfig};

/// Matches the window budget given to the other implementations, so no run is flow-control bound.
const STREAM_WINDOW: u64 = 32 * 1024 * 1024;

/// Carries the mode to the server, exactly as the h3 harness does.
const MODE_HEADER: &[u8] = b"x-tunnel-mode";

/// The authority of the CONNECT request. Nothing is tunneled to it; the server is the far end.
const TUNNEL_TARGET: &[u8] = b"tunnel.example:443";

pub struct Quiche {
    accept: tokio_quiche::QuicConnectionStream<DefaultMetrics>,
    addr: SocketAddr,
}

impl Quiche {
    pub fn new() -> Result<Self> {
        let socket =
            std::net::UdpSocket::bind("127.0.0.1:0").context("binding the server socket")?;
        let addr = socket.local_addr()?;

        let (cert, key) = cert_files()?;
        let params = ConnectionParams::new_server(
            quic_settings(),
            TlsCertificatePaths {
                cert,
                private_key: key,
                kind: CertificateKind::X509,
            },
            Hooks::default(),
        );

        let accept = listen(socket, params)?;

        Ok(Self { accept, addr })
    }

    /// Runs `rounds` transfers of every mode over a single connection, interleaved, and returns
    /// the timings grouped by mode.
    pub async fn run_series(
        &mut self,
        modes: &[Mode],
        cfg: RunConfig,
        rounds: usize,
    ) -> Result<Vec<Vec<Duration>>> {
        let Self { accept, addr } = self;
        let (report_tx, report_rx) = mpsc::unbounded_channel();
        let runs = modes.len() * rounds;

        let server = async {
            let conn = accept
                .next()
                .await
                .context("the listener is closed")?
                .context("accepting a connection")?;

            let (driver, mut controller) = ServerH3Driver::new(http3_settings());
            let _conn = conn.start(driver);

            serve(&mut controller, runs, report_tx).await
        };

        let client = async {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
            socket.connect(*addr).await?;

            // The client is the sender here, so its socket options decide the
            // ceiling. `tokio_quiche::listen` applies these for the server, but a client
            // `Socket` is built with every offload off and tokio-quiche's own `connect()`
            // deliberately leaves it that way ("some NICs don't support GSO"). Without
            // `UDP_SEGMENT` the send path flushes after every packet — one `sendmsg` per
            // ~1350 bytes — where quinn and s2n-quic batch by default, so the run would
            // measure syscall rate rather than protocol work.
            #[cfg_attr(not(target_os = "linux"), allow(unused_mut))]
            let mut socket = Socket::try_from(socket)?;
            #[cfg(target_os = "linux")]
            socket.apply_max_capabilities();

            let (driver, mut controller) = ClientH3Driver::new(http3_settings());
            let params = ConnectionParams::new_client(quic_settings(), None, Hooks::default());
            let _conn = connect_with_config(socket, Some("localhost"), &params, driver)
                .await
                .map_err(|err| anyhow::anyhow!("connecting: {err}"))?;

            transfer(&mut controller, modes, cfg, rounds, report_rx).await
        };

        let (served, transferred) = tokio::join!(server, client);

        // Report the client's error first: when one side gives up the other usually just sees its
        // channels close, which says much less about what went wrong.
        let timings = transferred?;
        served?;

        Ok(timings)
    }
}

/// Writes the test certificate out and returns the two paths.
///
/// boring takes its credentials as filenames, where the other two stacks take the PEM bytes
/// [`crate::certs`] has compiled in. The repository copy cannot simply be named: `baseline/` is
/// its own crate, so `CARGO_MANIFEST_DIR` points somewhere else there. Writing the same compiled
/// in bytes to a fixed temporary path works for both builds and leaves nothing to accumulate.
fn cert_files() -> Result<(&'static str, &'static str)> {
    static PATHS: OnceLock<(String, String)> = OnceLock::new();

    if PATHS.get().is_none() {
        let dir = std::env::temp_dir();
        let cert = dir.join("unbound-perf-cert.pem");
        let key = dir.join("unbound-perf-key.pem");

        fs::write(&cert, crate::certs::CERT_PEM).context("writing the test certificate")?;
        fs::write(&key, crate::certs::KEY_PEM).context("writing the test key")?;

        // It is a published demonstration key, but there is no reason to leave it readable to
        // every account on the machine.
        #[cfg(unix)]
        fs::set_permissions(&key, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .context("restricting the test key")?;

        let paths = (
            cert.to_str()
                .context("the temporary path is not UTF-8")?
                .to_owned(),
            key.to_str()
                .context("the temporary path is not UTF-8")?
                .to_owned(),
        );
        let _ = PATHS.set(paths);
    }

    let (cert, key) = PATHS.get().expect("just populated");

    Ok((cert, key))
}

/// Binds a listener for one connection.
///
/// Split out so the `remove(0)` on `listen`'s vector is not mixed into the constructor: it has
/// exactly one element because exactly one socket went in.
fn listen(
    socket: std::net::UdpSocket,
    params: ConnectionParams<'_>,
) -> Result<tokio_quiche::QuicConnectionStream<DefaultMetrics>> {
    let mut listeners =
        tokio_quiche::listen([socket], params, DefaultMetrics).context("starting the listener")?;

    Ok(listeners.remove(0))
}

fn quic_settings() -> QuicSettings {
    let mut settings = QuicSettings::default();

    // The benchmark trusts its own loopback peer; the certificate is only there to complete the
    // handshake.
    settings.verify_peer = false;
    settings.max_idle_timeout = Some(Duration::from_secs(30));
    settings.initial_max_data = 2 * STREAM_WINDOW;
    settings.initial_max_stream_data_bidi_local = STREAM_WINDOW;
    settings.initial_max_stream_data_bidi_remote = STREAM_WINDOW;
    settings.initial_max_stream_data_uni = STREAM_WINDOW;
    settings.max_connection_window = 2 * STREAM_WINDOW;
    settings.max_stream_window = STREAM_WINDOW;

    settings
}

fn http3_settings() -> Http3Settings {
    #[allow(unused_mut)]
    let mut settings = Http3Settings::default();

    #[cfg(feature = "unbound")]
    {
        settings.enable_unbound_data = true;
    }

    settings
}

/// Stops HTTP/3 framing on this endpoint's sending side, if the mode calls for it.
///
/// In the upstream baseline build only [`Mode::Framed`] exists, so the match has a single arm and
/// `send` goes unused.
#[cfg_attr(not(feature = "unbound"), allow(unused_variables))]
async fn send_switch(send: &mut OutboundFrameSender, mode: Mode) -> Result<()> {
    match mode {
        Mode::Framed => Ok(()),
        #[cfg(feature = "unbound")]
        Mode::Unbound | Mode::Handoff => send
            .send(OutboundFrame::UnboundData {
                take_over: mode == Mode::Handoff,
            })
            .await
            .map_err(|_| anyhow::anyhow!("the switch to unbound data was not accepted")),
    }
}

/// Accepts `runs` CONNECT tunnels and counts the bytes each one carries.
async fn serve(
    controller: &mut ServerH3Controller,
    runs: usize,
    reports: mpsc::UnboundedSender<u64>,
) -> Result<()> {
    let events = controller.event_receiver_mut();
    let mut served = 0;

    while served < runs {
        let Some(event) = events.recv().await else {
            bail!("the server driver stopped after {served} of {runs} runs");
        };

        let ServerH3Event::Headers {
            incoming_headers, ..
        } = event
        else {
            // Settings, byte accounting and stream closures are not of interest here.
            continue;
        };

        let mode = incoming_headers
            .headers
            .iter()
            .find(|header| header.name() == MODE_HEADER)
            .context("the request does not name a mode")?;
        let mode = Mode::parse(std::str::from_utf8(mode.value())?)?;

        let mut send = incoming_headers.send;
        let mut recv = incoming_headers.recv;

        send.send(OutboundFrame::Headers(
            vec![quiche::h3::Header::new(b":status", b"200")],
            None,
        ))
        .await
        .map_err(|_| anyhow::anyhow!("the response was not accepted"))?;

        // This endpoint sends nothing but the FIN, yet it still has to switch: the receiving side
        // of a direction is only unbound once the sender of that direction says so, and the
        // handoff needs both directions unbound before the driver can release the stream.
        send_switch(&mut send, mode).await?;

        let received = drain(&mut recv).await?;
        reports
            .send(received)
            .context("the client stopped listening")?;

        // Close this direction, so the stream and its driver state go away before the next run.
        send.send(OutboundFrame::Body(Bytes::new(), true))
            .await
            .map_err(|_| anyhow::anyhow!("the FIN was not accepted"))?;

        served += 1;
    }

    Ok(())
}

/// Counts the bytes the driver delivers, framed or not.
async fn drain(recv: &mut InboundFrameStream) -> Result<u64> {
    let mut total = 0;

    while let Some(frame) = recv.recv().await {
        match frame {
            InboundFrame::Body(body, fin) => {
                total += body.len() as u64;

                if fin {
                    return Ok(total);
                }
            }
            InboundFrame::Datagram(_) => bail!("unexpected datagram on a CONNECT stream"),
        }
    }

    Ok(total)
}

/// Opens `rounds` CONNECT tunnels of every mode on one connection, interleaved.
async fn transfer(
    controller: &mut ClientH3Controller,
    modes: &[Mode],
    cfg: RunConfig,
    rounds: usize,
    mut reports: mpsc::UnboundedReceiver<u64>,
) -> Result<Vec<Vec<Duration>>> {
    // One allocation up front; every write is a cheap refcounted slice of it.
    let payload = Bytes::from(vec![0x5a_u8; cfg.chunk]);
    let mut timings = vec![Vec::with_capacity(rounds); modes.len()];
    let mut request_id = 0;

    // No explicit wait for the peer's SETTINGS here, even though they decide whether this
    // endpoint may switch a stream. The driver forwards them lazily, piggybacked on the next
    // `h3::poll()` event, so a client that waits for the event before sending its first request
    // waits forever. The response headers are the barrier instead: the peer puts SETTINGS on its
    // control stream before it can answer anything, and loopback delivers in order.
    for _ in 0..rounds {
        for (index, &mode) in modes.iter().enumerate() {
            request_id += 1;

            let elapsed = run_once(controller, mode, cfg, &payload, request_id, &mut reports)
                .await
                .with_context(|| format!("{} run {request_id}", mode.label()))?;

            timings[index].push(elapsed);
        }
    }

    Ok(timings)
}

async fn run_once(
    controller: &mut ClientH3Controller,
    mode: Mode,
    cfg: RunConfig,
    payload: &Bytes,
    request_id: u64,
    reports: &mut mpsc::UnboundedReceiver<u64>,
) -> Result<Duration> {
    let (body_tx, body_rx) = oneshot::channel();

    controller
        .request_sender()
        .send(NewClientRequest {
            request_id,
            headers: vec![
                quiche::h3::Header::new(b":method", b"CONNECT"),
                quiche::h3::Header::new(b":authority", TUNNEL_TARGET),
                quiche::h3::Header::new(MODE_HEADER, mode.label().as_bytes()),
            ],
            body_writer: Some(body_tx),
        })
        .map_err(|_| anyhow::anyhow!("the request was not accepted"))?;

    let mut send = body_rx.await.context("no body writer for the request")?;

    // Everything above is setup; the tunnel is only up once the response is in.
    let mut recv = recv_response(controller).await?;

    let started = Instant::now();

    send_switch(&mut send, mode).await?;

    let mut sent = 0;
    while sent < cfg.total_bytes {
        let len = cfg.chunk.min((cfg.total_bytes - sent) as usize);
        let fin = sent + len as u64 == cfg.total_bytes;

        send.send(OutboundFrame::Body(payload.slice(..len), fin))
            .await
            .map_err(|_| anyhow::anyhow!("the tunnel stopped accepting data"))?;

        sent += len as u64;
    }

    // The far end counting the last byte is what ends the measurement, not this end handing the
    // last write to the driver.
    let received = reports
        .recv()
        .await
        .context("the far end stopped reporting")?;
    let elapsed = started.elapsed();

    if received != cfg.total_bytes {
        bail!(
            "the tunnel delivered {received} bytes, expected {}",
            cfg.total_bytes
        );
    }

    // Outside the clock: let the far end's FIN land so the stream is gone before the next run
    // reuses the connection. Any early close is equally fine — the payload is already accounted
    // for, and the next run opens a fresh stream.
    while recv.recv().await.is_some() {}

    Ok(elapsed)
}

async fn recv_response(controller: &mut ClientH3Controller) -> Result<InboundFrameStream> {
    let events = controller.event_receiver_mut();

    while let Some(event) = events.recv().await {
        match event {
            ClientH3Event::Core(H3Event::IncomingHeaders(headers)) => return Ok(headers.recv),
            ClientH3Event::Core(H3Event::ConnectionError(err)) => {
                bail!("connection error: {err}")
            }
            ClientH3Event::Core(H3Event::ConnectionShutdown(err)) => {
                bail!("connection shut down: {err:?}")
            }
            _ => continue,
        }
    }

    bail!("the client driver stopped before the response arrived")
}
