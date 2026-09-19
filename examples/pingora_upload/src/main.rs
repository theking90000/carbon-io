//! Streaming PUTs over HTTP/1.1 or HTTPS, sharing Pingora's connection pool.
//! Run: cargo run --manifest-path examples/pingora_upload/Cargo.toml -- <PUT URL>
//! Each URL receives "hello world\n" twice, in three Bytes frames per request.
//! The endpoint must accept chunked uploads and read the body before responding.

use bytes::Bytes;
use carbon_io::{FrameBudget, FrameWriter, SchedulerConfig, Window, WriteFile, WriteScheduler};
use futures::{StreamExt, stream};
use http::Uri;
use pingora_core::{
    connectors::http::Connector,
    protocols::http::client::HttpSession,
    upstreams::peer::HttpPeer,
};
use pingora_http::RequestHeader;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(30);
type Operation<T> = Pin<Box<dyn Future<Output = io::Result<T>>>>;

fn operation<T: 'static>(future: impl Future<Output = io::Result<T>> + 'static) -> Operation<T> {
    Box::pin(async move {
        tokio::time::timeout(TIMEOUT, future)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::TimedOut, error))?
    })
}

struct HttpFile {
    connector: Arc<Connector>,
    uri: Uri,
}

enum State {
    Idle(HttpSession),
    Writing(Operation<HttpSession>),
    Finalizing(Operation<u16>),
    Done,
}

struct HttpWriter {
    connector: Arc<Connector>,
    peer: HttpPeer,
    state: State,
}

impl WriteFile<Bytes> for HttpFile {
    type Error = io::Error;
    type Output = u16;
    type Open = Operation<HttpWriter>;
    type Writer = HttpWriter;

    fn frame_capacity(&self) -> u32 {
        3
    }

    fn open(&self) -> Self::Open {
        let connector = self.connector.clone();
        let uri = self.uri.clone();
        operation(async move {
            let tls = match uri.scheme_str() {
                Some("https") => true,
                Some("http") => false,
                _ => return Err(io::Error::other("expected an http:// or https:// URL")),
            };
            let host = uri.host().ok_or_else(|| io::Error::other("missing host"))?;
            let host = host.trim_start_matches('[').trim_end_matches(']');
            let port = uri.port_u16().unwrap_or(if tls { 443 } else { 80 });
            let authority = uri.authority().ok_or_else(|| io::Error::other("missing authority"))?;
            if authority.as_str().contains('@') {
                return Err(io::Error::other("credentials in URLs are not supported"));
            }
            // This example uses the first resolved address, without address failover.
            let address = tokio::net::lookup_host((host, port))
                .await?
                .next()
                .ok_or_else(|| io::Error::other("DNS returned no addresses"))?;
            let mut peer = HttpPeer::new(address, tls, host.to_owned());
            peer.options.set_http_version(1, 1);
            peer.options.connection_timeout = Some(TIMEOUT);
            // Keep Pingora's certificate and hostname verification enabled.
            let (mut session, reused) = connector
                .get_http_session(&peer)
                .await
                .map_err(io::Error::other)?;
            println!("PUT {uri} (reused connection: {reused})");

            let path = uri.path_and_query().map_or("/", |path| path.as_str());
            let mut request = RequestHeader::build("PUT", path.as_bytes(), Some(3))
                .map_err(io::Error::other)?;
            request.insert_header("Host", authority.as_str()).map_err(io::Error::other)?;
            request.insert_header("Content-Type", "application/octet-stream")
                .map_err(io::Error::other)?;
            request.insert_header("Transfer-Encoding", "chunked")
                .map_err(io::Error::other)?;
            session.write_request_header(Box::new(request)).await.map_err(io::Error::other)?;

            Ok(HttpWriter { connector, peer, state: State::Idle(session) })
        })
    }
}

impl FrameWriter<Bytes> for HttpWriter {
    type Error = io::Error;
    type Output = u16;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Bytes,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if matches!(&this.state, State::Idle(_)) {
            let State::Idle(mut session) = std::mem::replace(&mut this.state, State::Done) else {
                unreachable!();
            };
            // One shared Bytes handle per frame. Never capture the borrowed &Bytes.
            let bytes = frame.clone();
            this.state = State::Writing(operation(async move {
                session.write_request_body(bytes, false).await.map_err(io::Error::other)?;
                Ok(session)
            }));
        }
        let State::Writing(future) = &mut this.state else {
            return Poll::Ready(Err(io::Error::other("write after finalization")));
        };
        // Pending preserves the future, its session, and its Bytes handle.
        // CARBON presents the same logical frame, but we do not submit it again.
        match future.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(session)) => {
                this.state = State::Idle(session);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.state = State::Done;
                Poll::Ready(Err(error))
            }
        }
    }

    fn poll_finalize(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u16>> {
        let this = self.get_mut();
        if matches!(&this.state, State::Idle(_)) {
            let State::Idle(mut session) = std::mem::replace(&mut this.state, State::Done) else {
                unreachable!();
            };
            let connector = this.connector.clone();
            let peer = this.peer.clone();
            this.state = State::Finalizing(operation(async move {
                session.finish_request_body().await.map_err(io::Error::other)?;
                let status = loop {
                    session.read_response_header().await.map_err(io::Error::other)?;
                    let status = session.response_header()
                        .ok_or_else(|| io::Error::other("missing response headers"))?
                        .status.as_u16();
                    if status == 101 {
                        return Err(io::Error::other("unexpected protocol upgrade"));
                    }
                    if status >= 200 {
                        break status;
                    }
                };
                // Drain the response before returning the connection to the pool.
                while session.read_response_body().await.map_err(io::Error::other)?.is_some() {}
                // Pingora checks framing and keep-alive eligibility before reuse.
                connector.release_http_session(session, &peer, Some(TIMEOUT)).await;
                if !(200..300).contains(&status) {
                    return Err(io::Error::other(format!("upload returned HTTP {status}")));
                }
                Ok(status)
            }));
        }
        let State::Finalizing(future) = &mut this.state else {
            return Poll::Ready(Err(io::Error::other("cannot finalize in this state")));
        };
        match future.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.state = State::Done;
                Poll::Ready(result)
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls = std::env::args().skip(1)
        .map(|url| url.parse::<Uri>())
        .collect::<Result<Vec<_>, _>>()?;
    if urls.is_empty() {
        return Err(io::Error::other("usage: pingora_upload <PUT URL> [PUT URL ...]").into());
    }
    let connector = Arc::new(Connector::new(None));
    let files = stream::iter((0..2).flat_map(|_| urls.iter().cloned()).map(|uri| HttpFile {
        connector: connector.clone(),
        uri,
    }));
    let input = stream::iter((0..2 * urls.len()).flat_map(|_| [
        Bytes::from_static(b"hello"),
        Bytes::from_static(b" "),
        Bytes::from_static(b"world\n"),
    ]));
    // One active upload makes sequential connection reuse visible. Increase this
    // limit to upload concurrently; all writers still share the same pool.
    let config = SchedulerConfig::new(Window::new(16, 16, 1)?, 0);
    let mut uploads = WriteScheduler::new(input, files, FrameBudget::new(16), config);
    while let Some(result) = uploads.next().await {
        println!("upload completed: HTTP {}", result?);
    }
    Ok(())
}
