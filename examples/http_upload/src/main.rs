//! One streaming PUT per destination, with three Bytes frames per upload.
//! Run: cargo run --manifest-path examples/http_upload/Cargo.toml -- http://localhost:8000/object
//! The endpoint must accept streaming PUT bodies and reply after reading EOF.
//! Frame boundaries are not preserved by HTTP; the body concatenates the bytes.
//!
//! The adapter queues at most one extra frame per upload, outside FrameBudget.
//! Reqwest and the network stack also have their own buffers. Ready from
//! poll_write means queued locally, not acknowledged by the server.
//! No task is spawned by this adapter; Reqwest manages its transport internally.
//! Retries are disabled here. Remote writes cannot be undone by dropping a writer.

use bytes::Bytes;
use carbon_io::{FrameBudget, FrameWriter, SchedulerConfig, WriteFile, WriteScheduler};
use futures::{Stream, StreamExt, channel::mpsc, stream};
use reqwest::{Body, Client, StatusCode};
use std::{
    future::{Future, Ready, ready},
    io,
    pin::Pin,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    task::{Context, Poll, ready as poll_ready},
    time::Duration,
};

struct HttpFile {
    client: Client,
    url: String,
    frames: u32,
}

struct HttpWriter {
    sender: Option<mpsc::Sender<Bytes>>,
    request: Pin<Box<dyn Future<Output = io::Result<StatusCode>> + Send>>,
}

impl WriteFile<Bytes> for HttpFile {
    type Error = io::Error;
    type Output = StatusCode;
    type Open = Ready<io::Result<HttpWriter>>;
    type Writer = HttpWriter;

    fn frame_capacity(&self) -> u32 {
        self.frames
    }

    fn open(&self) -> Self::Open {
        // futures mpsc reserves one slot per sender, even with buffer = 0.
        // Keep exactly one sender, so the queue holds at most one frame.
        let (sender, mut receiver) = mpsc::channel::<Bytes>(0);
        let eof = Arc::new(AtomicBool::new(false));
        let body_eof = eof.clone();
        let body = stream::poll_fn(move |cx| {
            match poll_ready!(Pin::new(&mut receiver).poll_next(cx)) {
                Some(frame) => Poll::Ready(Some(Ok::<_, io::Error>(frame))),
                None => {
                    body_eof.store(true, Ordering::Release);
                    Poll::Ready(None)
                }
            }
        });
        let request = self.client.put(&self.url)
            .header("content-type", "application/octet-stream")
            .body(Body::wrap_stream(body));

        ready(Ok(HttpWriter {
            sender: Some(sender),
            request: Box::pin(async move {
                let response = request.send().await.map_err(io::Error::other)?;
                let status = response.status();
                if !status.is_success() {
                    return Err(io::Error::other(format!("upload returned HTTP {status}")));
                }
                // An early 2xx must not silently accept a truncated upload.
                if !eof.load(Ordering::Acquire) {
                    return Err(io::Error::other("server replied before body EOF"));
                }
                Ok(status)
            }),
        }))
    }
}

impl FrameWriter<Bytes> for HttpWriter {
    type Error = io::Error;
    type Output = StatusCode;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Bytes,
    ) -> Poll<io::Result<()>> {
        
        let this = self.get_mut();
        // Start/advance the SAME request, and detect HTTP failures while writing.
        if let Poll::Ready(result) = this.request.as_mut().poll(cx) {
            return Poll::Ready(Err(match result {
                Err(error) => error,
                Ok(_) => io::Error::other("request completed before finalization"),
            }));
        }

        let sender = this.sender.as_mut().expect("write before finalize");
        // Pending here has NOT accepted this frame. CARBON will present it again.
        poll_ready!(sender.poll_ready(cx)).map_err(io::Error::other)?;
        println!("|> poll_write: {} bytes id:{:p}", frame.len(), sender);
        // Clone the Bytes handle once, without copying its contents or keeping &Bytes.
        // Ready immediately follows acceptance: the next call is a new frame.
        Poll::Ready(sender.start_send(frame.clone()).map_err(io::Error::other))
    }

    fn poll_finalize(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<StatusCode>> {
        let this = self.get_mut();
        // Closing the only sender drains the queued frame, then yields body EOF.
        // take() makes this safe across repeated Pending polls.
        drop(this.sender.take());
        this.request.as_mut().poll(cx)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls: Vec<String> = std::env::args().skip(1).collect();
    if urls.is_empty() {
        return Err(io::Error::other("usage: http_upload <PUT URL> [PUT URL ...]").into());
    }
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(60))
        .build()?;
    println!("Client initialized. {} upload(s) to perform.", urls.len());
    let count = urls.len();
    let files = stream::iter(urls.into_iter().map(|url| HttpFile {
        client: client.clone(),
        url,
        frames: 3,
    }));
    // Replace this lazy input with any Stream<Item = Bytes>.
    let input = stream::iter((0..count).flat_map(|_| [
        Bytes::from_static(b"hello"),
        Bytes::from_static(b" "),
        Bytes::from_static(b"world\n"),
    ]));
    let config = SchedulerConfig::default().with_max_retries(0);
    let mut uploads = WriteScheduler::new(input, files, FrameBudget::new(16), config);
    while let Some(result) = uploads.next().await {
        println!("upload completed: {}", result?);
    }
    Ok(())
}
