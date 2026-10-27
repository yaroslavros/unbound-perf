//! quinn endpoints and the native-stream glue for the handoff mode.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    certs,
    tunnel::{self, Mode, NativeHalves, NativeRecv, NativeSend, RunConfig},
};

/// Generous enough that a bulk transfer is never flow-control bound, so what is measured is the
/// protocol work rather than window sizing. The same budget is given to s2n-quic.
const STREAM_WINDOW: u64 = 32 * 1024 * 1024;

pub struct Quinn {
    server: quinn::Endpoint,
    client: quinn::Endpoint,
    addr: SocketAddr,
}

impl Quinn {
    pub fn new() -> Result<Self> {
        let mut server_crypto = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(certs::chain()?, certs::key()?)?;
        server_crypto.alpn_protocols = vec![b"h3".to_vec()];

        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?,
        ));
        server_config.transport = transport();

        let server = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse()?)?;
        let addr = server.local_addr()?;

        let mut client_crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(certs::root_store()?)
        .with_no_client_auth();
        client_crypto.alpn_protocols = vec![b"h3".to_vec()];

        let mut client_config = quinn::ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)?,
        ));
        client_config.transport_config(transport());

        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse()?)?;
        client.set_default_client_config(client_config);

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
                .context("the server endpoint is closed")?
                .await?;

            tunnel::serve(
                h3_quinn::Connection::new(connection),
                modes.len() * rounds,
                report_tx,
            )
            .await
        };

        let transfer = async {
            let connection = client.connect(*addr, "localhost")?.await?;

            tunnel::transfer(
                h3_quinn::Connection::new(connection),
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

fn transport() -> Arc<quinn::TransportConfig> {
    let mut transport = quinn::TransportConfig::default();
    transport
        .max_idle_timeout(Some(
            Duration::from_secs(30)
                .try_into()
                .expect("30s is a valid idle timeout"),
        ))
        .stream_receive_window(STREAM_WINDOW.try_into().expect("valid window"))
        .receive_window((2 * STREAM_WINDOW).try_into().expect("valid window"))
        .send_window(2 * STREAM_WINDOW);

    Arc::new(transport)
}

#[cfg(feature = "unbound")]
impl NativeHalves for h3_quinn::BidiStream<Bytes> {
    type NativeSender = quinn::SendStream;
    type NativeReceiver = quinn::RecvStream;

    fn into_native_send(send: Self::SendStream) -> Self::NativeSender {
        send.into_quinn()
            .unwrap_or_else(|_| panic!("h3 flushes the stream before handing it over"))
    }

    fn into_native_recv(recv: Self::RecvStream) -> Self::NativeReceiver {
        recv.into_quinn()
    }
}

/// The upstream h3-quinn has no `into_quinn`, so the handoff mode is not available there.
#[cfg(not(feature = "unbound"))]
impl NativeHalves for h3_quinn::BidiStream<Bytes> {
    type NativeSender = crate::tunnel::Unsupported;
    type NativeReceiver = crate::tunnel::Unsupported;

    fn into_native_send(_send: Self::SendStream) -> Self::NativeSender {
        crate::tunnel::Unsupported
    }

    fn into_native_recv(_recv: Self::RecvStream) -> Self::NativeReceiver {
        crate::tunnel::Unsupported
    }
}

impl NativeSend for quinn::SendStream {
    async fn send_chunk(&mut self, chunk: Bytes) -> Result<()> {
        self.write_chunk(chunk).await?;

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        quinn::SendStream::finish(self)?;

        Ok(())
    }
}

impl NativeRecv for quinn::RecvStream {
    async fn recv_chunk(&mut self) -> Result<Option<usize>> {
        Ok(self
            .read_chunk(usize::MAX, true)
            .await?
            .map(|chunk| chunk.bytes.len()))
    }
}
