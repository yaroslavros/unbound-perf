//! s2n-quic endpoints and the native-stream glue for the handoff mode.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use s2n_quic::{client::Connect, provider::limits::Limits};
use tokio::sync::mpsc;

use crate::{
    certs,
    tunnel::{self, Mode, NativeHalves, NativeRecv, NativeSend, RunConfig},
};

/// Matches the window budget given to quinn, so neither run is flow-control bound.
const STREAM_WINDOW: u64 = 32 * 1024 * 1024;

pub struct S2nQuic {
    server: s2n_quic::Server,
    client: s2n_quic::Client,
    addr: SocketAddr,
}

impl S2nQuic {
    pub fn new() -> Result<Self> {
        let server = s2n_quic::Server::builder()
            .with_tls((certs::CERT_PEM, certs::KEY_PEM))?
            .with_io("127.0.0.1:0")?
            .with_limits(limits()?)?
            .start()?;
        let addr = server.local_addr()?;

        let client = s2n_quic::Client::builder()
            .with_tls(certs::CERT_PEM)?
            .with_io("127.0.0.1:0")?
            .with_limits(limits()?)?
            .start()?;

        Ok(Self {
            server,
            client,
            addr,
        })
    }

    /// Runs `rounds` transfers of every mode over a single connection, interleaved, and returns
    /// the timings grouped by mode.
    pub async fn run_series(
        &mut self,
        modes: &[Mode],
        cfg: RunConfig,
        rounds: usize,
    ) -> Result<Vec<Vec<Duration>>> {
        let Self {
            server,
            client,
            addr,
        } = self;
        let (report_tx, report_rx) = mpsc::unbounded_channel();

        let serve = async {
            let connection = server
                .accept()
                .await
                .context("the server endpoint is closed")?;

            tunnel::serve(
                s2n_quic_h3::Connection::new(connection),
                modes.len() * rounds,
                report_tx,
            )
            .await
        };

        let transfer = async {
            let connection = client
                .connect(Connect::new(*addr).with_server_name("localhost"))
                .await?;

            tunnel::transfer(
                s2n_quic_h3::Connection::new(connection),
                modes,
                cfg,
                rounds,
                report_rx,
            )
            .await
        };

        let (served, transferred) = tokio::join!(serve, transfer);
        served?;

        transferred
    }
}

fn limits() -> Result<Limits> {
    Ok(Limits::default()
        .with_data_window(2 * STREAM_WINDOW)?
        .with_bidirectional_local_data_window(STREAM_WINDOW)?
        .with_bidirectional_remote_data_window(STREAM_WINDOW)?
        .with_unidirectional_data_window(STREAM_WINDOW)?
        .with_max_send_buffer_size(2 * 1024 * 1024)?
        .with_max_idle_timeout(Duration::from_secs(30))?)
}

impl NativeHalves for s2n_quic_h3::BidiStream<Bytes> {
    type NativeSender = s2n_quic::stream::SendStream;
    type NativeReceiver = s2n_quic::stream::ReceiveStream;

    fn into_native_send(send: Self::SendStream) -> Self::NativeSender {
        send.into_s2n_quic()
            .unwrap_or_else(|_| panic!("h3 flushes the stream before handing it over"))
    }

    fn into_native_recv(recv: Self::RecvStream) -> Self::NativeReceiver {
        recv.into_s2n_quic()
    }
}

impl NativeSend for s2n_quic::stream::SendStream {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<()> {
        self.send(chunk).await?;

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        s2n_quic::stream::SendStream::finish(self)?;

        Ok(())
    }
}

impl NativeRecv for s2n_quic::stream::ReceiveStream {
    async fn recv_chunk(&mut self) -> Result<Option<usize>> {
        Ok(self.receive().await?.map(|chunk| chunk.len()))
    }
}
