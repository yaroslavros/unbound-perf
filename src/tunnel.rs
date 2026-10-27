//! The measurement itself, written once and shared by both QUIC backends.
//!
//! Everything below is generic over `h3::quic::Connection`, so the framed and the unbound runs
//! execute the exact same code for both libraries; the only thing that changes between modes is
//! whether HTTP/3 framing is in the path.
//!
//! The transfer is one-way, client to server, which is what a CONNECT upload looks like. Each
//! side therefore switches only the direction it uses: the client sends `UNBOUND_DATA` and the
//! server only ever receives it. The draft allows exactly that, and it keeps the server off the
//! "has the peer's SETTINGS frame arrived yet" path entirely.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::{Buf, Bytes};
#[cfg(feature = "unbound")]
use h3::error::StreamError;
use http::{Method, Request, Response, StatusCode};
use tokio::sync::mpsc;

/// The authority of the CONNECT request. Nothing is tunneled to it; the server is the far end.
const TUNNEL_TARGET: &str = "tunnel.example:443";

/// Carries the mode to the server, so one connection can serve all three interleaved. Measuring
/// the modes in the same time window is what keeps a drifting machine from biasing one against
/// another.
const MODE_HEADER: &str = "x-tunnel-mode";

/// How the CONNECT tunnel carries its payload.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    /// Plain RFC 9114 CONNECT: every write is wrapped in a `DATA` frame.
    ///
    /// The only mode an unmodified h3 can do, which is why it is the one the upstream baseline
    /// build measures.
    Framed,
    /// `UNBOUND_DATA` is sent, but the stream stays with h3: writes and reads still go through
    /// `send_data`/`recv_data`, just without frame headers.
    #[cfg(feature = "unbound")]
    Unbound,
    /// `UNBOUND_DATA` is sent and the stream is handed back to the QUIC implementation, so h3 is
    /// out of the data path entirely and the payload moves with the native API.
    #[cfg(feature = "unbound")]
    Handoff,
}

impl Mode {
    pub fn label(self) -> &'static str {
        match self {
            Mode::Framed => "framed",
            #[cfg(feature = "unbound")]
            Mode::Unbound => "unbound",
            #[cfg(feature = "unbound")]
            Mode::Handoff => "handoff",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "framed" => Ok(Mode::Framed),
            #[cfg(feature = "unbound")]
            "unbound" => Ok(Mode::Unbound),
            #[cfg(feature = "unbound")]
            "handoff" => Ok(Mode::Handoff),
            #[cfg(not(feature = "unbound"))]
            "unbound" | "handoff" => bail!(
                "`{value}` needs the unbound data API; this binary is built against the \
                 unmodified upstream libraries, which only have framed CONNECT"
            ),
            other => bail!("unknown mode `{other}`, expected framed, unbound or handoff"),
        }
    }
}

/// One transfer's parameters.
#[derive(Clone, Copy, Debug)]
pub struct RunConfig {
    pub total_bytes: u64,
    pub chunk: usize,
}

impl RunConfig {
    /// How many bytes of `DATA` frame headers a framed run spends on this configuration.
    pub fn framing_overhead(&self) -> u64 {
        let chunks = self.total_bytes.div_ceil(self.chunk as u64);
        // DATA frame header: type 0x00 as a one byte varint, then the length as a varint.
        let length_bytes = match self.chunk as u64 {
            n if n < 1 << 6 => 1,
            n if n < 1 << 14 => 2,
            n if n < 1 << 30 => 4,
            _ => 8,
        };

        chunks * (1 + length_bytes)
    }
}

/// Converts the halves of a stream h3 handed back into the QUIC implementation's own types.
///
/// Both implementations are given the same shape of API — one owned [`Bytes`] per write, one
/// owned chunk per read — which is also what the h3 backends use internally. That keeps the
/// framed and handoff numbers comparable: no mode pays for a copy the other avoids.
#[cfg_attr(not(feature = "unbound"), allow(dead_code))]
pub trait NativeHalves: h3::quic::BidiStream<Bytes> + Sized {
    type NativeSender: NativeSend;
    type NativeReceiver: NativeRecv;

    fn into_native_send(send: Self::SendStream) -> Self::NativeSender;
    fn into_native_recv(recv: Self::RecvStream) -> Self::NativeReceiver;
}

/// Stands in for the native stream types in the upstream baseline build, where the handoff API
/// does not exist. The modes that would use it cannot be selected there, so it is never built.
#[cfg(not(feature = "unbound"))]
pub struct Unsupported;

#[cfg(not(feature = "unbound"))]
impl NativeSend for Unsupported {
    async fn send_chunk(&mut self, _chunk: Bytes) -> Result<()> {
        unreachable!("the handoff mode needs the unbound data API")
    }

    fn finish(&mut self) -> Result<()> {
        unreachable!("the handoff mode needs the unbound data API")
    }
}

#[cfg(not(feature = "unbound"))]
impl NativeRecv for Unsupported {
    async fn recv_chunk(&mut self) -> Result<Option<usize>> {
        unreachable!("the handoff mode needs the unbound data API")
    }
}

#[allow(async_fn_in_trait)]
#[cfg_attr(not(feature = "unbound"), allow(dead_code))]
pub trait NativeSend {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

#[allow(async_fn_in_trait)]
#[cfg_attr(not(feature = "unbound"), allow(dead_code))]
pub trait NativeRecv {
    /// Returns the size of the next chunk, or `None` at the end of the stream.
    async fn recv_chunk(&mut self) -> Result<Option<usize>>;
}

/// Accepts `runs` CONNECT tunnels on one connection and counts the bytes each one carries.
///
/// Each request names its own mode, so the caller decides the order they run in.
pub async fn serve<C>(conn: C, runs: usize, reports: mpsc::UnboundedSender<u64>) -> Result<()>
where
    C: h3::quic::Connection<Bytes>,
    C::BidiStream: NativeHalves,
{
    #[cfg_attr(not(feature = "unbound"), allow(unused_mut))]
    let mut builder = h3::server::builder();
    #[cfg(feature = "unbound")]
    builder.enable_unbound_data(true);
    let mut incoming: h3::server::Connection<C, Bytes> = builder.build(conn).await?;

    for _ in 0..runs {
        let resolver = incoming.accept().await?.context("no request")?;
        let (request, mut stream) = resolver.resolve_request().await?;
        if request.method() != Method::CONNECT {
            bail!("expected a CONNECT request, got {}", request.method());
        }

        let mode = request
            .headers()
            .get(MODE_HEADER)
            .context("the request does not name a mode")?
            .to_str()
            .context("the mode header is not valid text")?;
        let mode = Mode::parse(mode)?;

        stream.send_response(Response::new(())).await?;

        let received = match mode {
            // Unbound data is transparent to `recv_data`, so both of these drain the same way.
            Mode::Framed => drain_through_h3(&mut stream).await?,
            #[cfg(feature = "unbound")]
            Mode::Unbound => drain_through_h3(&mut stream).await?,
            #[cfg(feature = "unbound")]
            Mode::Handoff => {
                // Only the receiving direction is taken over. `_send` stays alive because
                // dropping it would reset that direction under the peer.
                let (_send, recv) = stream.split();
                let unbound = recv.into_unbound_recv().await?;
                let mut total = unbound.prefix.remaining() as u64;

                let mut native = C::BidiStream::into_native_recv(unbound.stream);
                while let Some(len) = native.recv_chunk().await? {
                    total += len as u64;
                }
                total
            }
        };

        reports
            .send(received)
            .context("the client stopped listening")?;
    }

    Ok(())
}

/// Counts the bytes h3 delivers, whether they arrived framed or unbound.
async fn drain_through_h3<S>(stream: &mut h3::server::RequestStream<S, Bytes>) -> Result<u64>
where
    S: h3::quic::RecvStream,
{
    let mut total = 0;
    while let Some(chunk) = stream.recv_data().await? {
        total += chunk.remaining() as u64;
    }

    Ok(total)
}

/// Runs `rounds` transfers of every mode on one connection, interleaved, and returns the timings
/// grouped by mode.
///
/// Reusing the connection keeps the congestion window warm, and interleaving the modes means a
/// machine that drifts mid-run affects all of them alike rather than whichever was measured last.
pub async fn transfer<C, O>(
    conn: C,
    modes: &[Mode],
    cfg: RunConfig,
    rounds: usize,
    reports: mpsc::UnboundedReceiver<u64>,
) -> Result<Vec<Vec<Duration>>>
where
    C: h3::quic::Connection<Bytes, OpenStreams = O>,
    O: h3::quic::OpenStreams<Bytes, BidiStream = C::BidiStream>,
    C::BidiStream: NativeHalves,
{
    #[cfg_attr(not(feature = "unbound"), allow(unused_mut))]
    let mut builder = h3::client::builder();
    #[cfg(feature = "unbound")]
    builder.enable_unbound_data(true);
    let (mut driver, send_request) = builder.build::<C, O, Bytes>(conn).await?;

    let series = run_series(send_request, modes, cfg, rounds, reports);

    tokio::select! {
        // Biased, because the far end closes the connection as soon as it has counted the last
        // byte of the last run: the series is complete by then and must not lose to the close.
        biased;
        result = series => result,
        error = std::future::poll_fn(|cx| driver.poll_close(cx)) => {
            bail!("connection closed during the transfer: {error}")
        }
    }
}

async fn run_series<O>(
    mut send_request: h3::client::SendRequest<O, Bytes>,
    modes: &[Mode],
    cfg: RunConfig,
    rounds: usize,
    mut reports: mpsc::UnboundedReceiver<u64>,
) -> Result<Vec<Vec<Duration>>>
where
    O: h3::quic::OpenStreams<Bytes>,
    O::BidiStream: NativeHalves,
{
    // One allocation up front; every write is a cheap refcounted slice of it.
    let payload = Bytes::from(vec![0x5a_u8; cfg.chunk]);
    let mut timings = vec![Vec::with_capacity(rounds); modes.len()];

    for _ in 0..rounds {
        for (index, &mode) in modes.iter().enumerate() {
            let elapsed = run_once(&mut send_request, mode, cfg, &payload, &mut reports).await?;
            timings[index].push(elapsed);
        }
    }

    Ok(timings)
}

async fn run_once<O>(
    send_request: &mut h3::client::SendRequest<O, Bytes>,
    mode: Mode,
    cfg: RunConfig,
    payload: &Bytes,
    reports: &mut mpsc::UnboundedReceiver<u64>,
) -> Result<Duration>
where
    O: h3::quic::OpenStreams<Bytes>,
    O::BidiStream: NativeHalves,
{
    let request = Request::connect(TUNNEL_TARGET)
        .header(MODE_HEADER, mode.label())
        .body(())?;
    let mut stream = send_request.send_request(request).await?;

    let response = stream.recv_response().await?;
    if response.status() != StatusCode::OK {
        bail!("CONNECT was refused with {}", response.status());
    }

    // Everything above is setup, so the clock starts here and stops once the far end has
    // accounted for the last byte.
    let started = Instant::now();

    match mode {
        Mode::Framed => {
            send_framed(&mut stream, payload, cfg).await?;
            stream.finish().await?;
        }
        #[cfg(feature = "unbound")]
        Mode::Unbound => {
            switch_to_unbound(&mut stream).await?;
            send_framed(&mut stream, payload, cfg).await?;
            stream.finish().await?;
        }
        #[cfg(feature = "unbound")]
        Mode::Handoff => {
            switch_to_unbound(&mut stream).await?;

            // Only the sending direction is taken over. `_recv` stays alive because dropping it
            // would ask the peer to stop sending on a stream still in use.
            let (send, _recv) = stream.split();
            let unbound = send.into_unbound_send().await?;

            let mut native = O::BidiStream::into_native_send(unbound.stream);
            let mut sent = 0;
            while sent < cfg.total_bytes {
                let len = remaining_chunk(&cfg, sent);
                native.send_chunk(payload.slice(..len)).await?;
                sent += len as u64;
            }
            native.finish()?;
        }
    }

    let received = reports
        .recv()
        .await
        .context("the far end stopped reporting")?;
    let elapsed = started.elapsed();

    if received != cfg.total_bytes {
        bail!(
            "tunnel delivered {received} bytes, expected {}",
            cfg.total_bytes
        );
    }

    Ok(elapsed)
}

/// Switches the sending side to unbound data, waiting out the peer's SETTINGS frame.
#[cfg(feature = "unbound")]
///
/// The control stream is unordered with respect to the request stream, so the client can be ready
/// to switch before it knows whether the server accepts `UNBOUND_DATA`. Only the first run of a
/// series ever waits here.
async fn switch_to_unbound<S>(stream: &mut h3::client::RequestStream<S, Bytes>) -> Result<()>
where
    S: h3::quic::SendStream<Bytes>,
{
    loop {
        match stream.send_unbound_data().await {
            Ok(()) => return Ok(()),
            Err(StreamError::UnboundDataNotNegotiated) => {
                tokio::time::sleep(Duration::from_millis(1)).await
            }
            Err(err) => return Err(err).context("send_unbound_data"),
        }
    }
}

/// Writes the payload through h3, which frames it unless the stream is already unbound.
async fn send_framed<S>(
    stream: &mut h3::client::RequestStream<S, Bytes>,
    payload: &Bytes,
    cfg: RunConfig,
) -> Result<()>
where
    S: h3::quic::SendStream<Bytes>,
{
    let mut sent = 0;
    while sent < cfg.total_bytes {
        let len = remaining_chunk(&cfg, sent);
        stream.send_data(payload.slice(..len)).await?;
        sent += len as u64;
    }

    Ok(())
}

fn remaining_chunk(cfg: &RunConfig, sent: u64) -> usize {
    cfg.chunk.min((cfg.total_bytes - sent) as usize)
}
