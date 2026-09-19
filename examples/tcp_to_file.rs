//! TCP upload -> CARBON -> real files -> CARBON -> TCP download.
//! Run with `cargo run --example tcp_to_file`; instructions are printed on startup.
use carbon_io::{
    FrameBudget, FrameWriter, ReadFile, ReadScheduler, SchedulerConfig, Window, WriteFile,
    WriteScheduler,
};
use futures::{Stream, StreamExt, stream};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Sleep, sleep},
};

const BLOCK: usize = 1 << 16; // 64 KiB per frame.
const FRAMES_PER_FILE: u32 = 160; // 10 MiB per complete file.
const PIPELINE_FILES: usize = 2; // Discovery horizon and active-file limit.
const PIPELINE_FRAMES: usize = 400; // Frame budget and local buffering target.
type Frame = Vec<u8>;

struct Segment {
    path: PathBuf,
    frames: u32,
}

struct Writer {
    file: File,
    offset: usize,
    path: PathBuf,
    finalize_delay: Option<Pin<Box<Sleep>>>,
}

impl WriteFile<Frame> for Segment {
    type Error = io::Error;
    type Output = PathBuf;
    type Writer = Writer;
    type Open = Pin<Box<dyn Future<Output = io::Result<Writer>>>>;

    fn frame_capacity(&self) -> u32 {
        self.frames
    }

    fn open(&self) -> Self::Open {
        let path = self.path.clone();
        Box::pin(async move {
            // CARBON calls this only once a frame is assigned to the segment.
            sleep(Duration::from_secs(1)).await;
            Ok(Writer {
                file: File::create(&path).await?,
                offset: 0,
                path,
                finalize_delay: None,
            })
        })
    }
}

impl FrameWriter<Frame> for Writer {
    type Error = io::Error;
    type Output = PathBuf;

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Frame,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.offset < frame.len() {
            let written = ready!(Pin::new(&mut this.file).poll_write(cx, &frame[this.offset..]))?;
            if written == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            this.offset += written;
        }
        this.offset = 0;
        Poll::Ready(Ok(()))
    }

    fn poll_finalize(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<PathBuf>> {
        let this = self.get_mut();
        ready!(Pin::new(&mut this.file).poll_flush(cx))?;
        let delay = this
            .finalize_delay
            .get_or_insert_with(|| Box::pin(sleep(Duration::from_secs(2))));
        ready!(delay.as_mut().poll(cx));
        Poll::Ready(Ok(this.path.clone()))
    }
}

impl ReadFile<Frame> for Segment {
    type Error = io::Error;
    type Reader = Pin<Box<dyn Stream<Item = io::Result<Frame>>>>;
    type Open = Pin<Box<dyn Future<Output = io::Result<Self::Reader>>>>;

    fn frame_count(&self) -> u32 {
        self.frames
    }

    fn open(&self) -> Self::Open {
        let path = self.path.clone();
        let frames = self.frames;
        Box::pin(async move {
            sleep(Duration::from_millis(500)).await;
            let file = File::open(path).await?;
            let reader = stream::try_unfold((file, frames), |(mut file, left)| async move {
                if left == 0 {
                    return Ok(None);
                }
                let mut frame = vec![0; BLOCK];
                file.read_exact(&mut frame).await?;
                Ok(Some((frame, (file, left - 1))))
            });
            Ok(Box::pin(reader) as Self::Reader)
        })
    }
}

fn config() -> SchedulerConfig {
    SchedulerConfig {
        window: Window {
            target_frames: PIPELINE_FRAMES,
            open_ahead_frames: PIPELINE_FILES * FRAMES_PER_FILE as usize,
            max_active_files: PIPELINE_FILES,
        },
        ..SchedulerConfig::default()
    }
}

async fn upload(socket: TcpStream, directory: &Path) -> io::Result<u64> {
    // ponytail: one transfer at a time; replace the previous upload in this owned directory.
    fs::remove_dir_all(directory).await?;
    fs::create_dir(directory).await?;
    let total = Cell::new(0u64);
    let failure = RefCell::new(None);
    let input = stream::unfold(socket, |mut socket| {
        let (total, failure) = (&total, &failure);
        async move {
            let mut frame = vec![0; BLOCK];
            let mut filled = 0;
            while filled < BLOCK {
                match socket.read(&mut frame[filled..]).await {
                    Ok(0) => break,
                    Ok(n) => filled += n,
                    Err(error) => {
                        *failure.borrow_mut() = Some(error);
                        return None;
                    }
                }
            }
            if filled == 0 {
                return None;
            }
            total.set(total.get() + filled as u64);
            Some((frame, socket))
        }
    });
    let destinations = stream::iter((0u64..).map(|index| Segment {
        path: directory.join(format!("{index:020}.bin")),
        frames: FRAMES_PER_FILE,
    }));
    let mut writer = WriteScheduler::new(
        input,
        destinations,
        FrameBudget::new(PIPELINE_FRAMES),
        config(),
    );
    while let Some(result) = writer.next().await {
        println!("Written {}", result.map_err(io::Error::other)?.display());
    }
    drop(writer);
    if let Some(error) = failure.into_inner() {
        return Err(error);
    }
    // Published only after every segment has been flushed and TCP reached EOF.
    fs::write(directory.join("stream.meta"), format!("{}\n", total.get())).await?;
    Ok(total.get())
}

async fn download(mut socket: TcpStream, directory: &Path) -> io::Result<u64> {
    let total: u64 = fs::read_to_string(directory.join("stream.meta"))
        .await?
        .trim()
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut entries = fs::read_dir(directory).await?;
    let mut files = Vec::new();
    let mut stored = 0u64;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "bin") {
            let size = entry.metadata().await?.len();
            if size == 0
                || size % BLOCK as u64 != 0
                || size > FRAMES_PER_FILE as u64 * BLOCK as u64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid segment size",
                ));
            }
            stored += size;
            files.push(Segment {
                path,
                frames: (size / BLOCK as u64) as u32,
            });
        }
    }
    if stored / BLOCK as u64 != total.div_ceil(BLOCK as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segments do not match stream.meta",
        ));
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let mut reader = ReadScheduler::new(
        stream::iter(files),
        FrameBudget::new(PIPELINE_FRAMES),
        config(),
    );
    let mut remaining = total;
    while let Some(frame) = reader.next().await {
        let frame = frame.map_err(io::Error::other)?;
        let length = remaining.min(BLOCK as u64) as usize;
        tokio::select! {
            result = socket.write_all(&frame[..length]) => result?,
            _ = reader.progress() => unreachable!("progress never completes"),
        }
        remaining -= length as u64;
    }
    socket.shutdown().await?;
    Ok(total)
}

async fn temporary_directory() -> io::Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let directory = PathBuf::from(format!("/tmp/carbon-io-tcp-{}-{stamp}", std::process::id()));
    fs::create_dir(&directory).await?;
    Ok(directory)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let uploads = TcpListener::bind("127.0.0.1:9000").await?;
    let downloads = TcpListener::bind("127.0.0.1:9001").await?;
    let directory = temporary_directory().await?;
    println!(
        "CARBON I/O: TCP to real files\n\
         Directory: {}\n\
         Frame size: {BLOCK} bytes. Frames per file: {FRAMES_PER_FILE}.\n\
         Pipeline: up to {PIPELINE_FILES} active files, {PIPELINE_FRAMES} buffered frames.\n\
         The last segment may be shorter; stream.meta stores the exact byte count.\n\
         One transfer at a time. Each new upload replaces the previous one.\n\n\
         Upload (Linux / OpenBSD nc): cat source.bin | nc -N 127.0.0.1 9000\n\
         Upload (macOS nc):           cat source.bin | nc -w 2 127.0.0.1 9000\n\
         Download:                   nc 127.0.0.1 9001 > restored.bin\n\
         Verify:                     cmp source.bin restored.bin\n\
         Inspect:                    ls -lh {}\n\
         Finish the upload with EOF and wait for 'Upload complete' before downloading.\n\
         Ctrl-C stops the server; files remain in the directory above.\n",
        directory.display(),
        directory.display()
    );
    loop {
        tokio::select! {
            connection = uploads.accept() => {
                let (socket, peer) = connection?;
                println!("Upload from {peer}");
                match upload(socket, &directory).await {
                    Ok(bytes) => println!("Upload complete: {bytes} bytes"),
                    Err(error) => eprintln!("Upload failed: {error}"),
                }
            }
            connection = downloads.accept() => {
                let (socket, _) = connection?;
                match download(socket, &directory).await {
                    Ok(bytes) => println!("Download complete: {bytes} bytes"),
                    Err(error) => eprintln!("Download failed: {error}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_round_trip_padding_and_replacement() -> io::Result<()> {
        let directory = temporary_directory().await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let file_size = FRAMES_PER_FILE as usize * BLOCK;
        for size in [0, 3 * BLOCK - 17, file_size, 2 * file_size + 13, 7, 0] {
            let original: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
            let mut client = TcpStream::connect(listener.local_addr()?).await?;
            let (server, _) = listener.accept().await?;
            let (stored, sent) = tokio::join!(upload(server, &directory), async {
                // TCP reads need not coincide with frame boundaries.
                for fragment in original.chunks(997) {
                    client.write_all(fragment).await?;
                }
                client.shutdown().await
            });
            sent?;
            assert_eq!(stored?, size as u64);
            let mut client = TcpStream::connect(listener.local_addr()?).await?;
            let (server, _) = listener.accept().await?;
            let mut restored = Vec::new();
            let (sent, received) = tokio::join!(
                download(server, &directory),
                client.read_to_end(&mut restored)
            );
            assert_eq!(sent?, size as u64);
            assert_eq!(received?, size);
            assert_eq!(restored, original);
            let mut entries = fs::read_dir(&directory).await?;
            let mut lengths = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                if entry.path().extension().is_some_and(|ext| ext == "bin") {
                    lengths.push(entry.metadata().await?.len());
                }
            }
            assert_eq!(
                lengths.iter().sum::<u64>(),
                (size.div_ceil(BLOCK) * BLOCK) as u64
            );
            assert!(
                lengths
                    .iter()
                    .all(|&n| n > 0 && n % BLOCK as u64 == 0 && n <= file_size as u64)
            );
            if size <= 3 * BLOCK {
                assert_eq!(lengths.len(), usize::from(size > 0));
            }
        }
        fs::remove_dir_all(directory).await
    }
}
