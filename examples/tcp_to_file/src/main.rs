//! TCP upload -> CARBON -> real files -> CARBON -> TCP download.
//! Run with `cargo run --manifest-path examples/tcp_to_file/Cargo.toml`; instructions are printed on startup.
use carbon_io::{
    AsyncFilesWrite, BytesFrameAllocator, FrameBudget, PushError, ReadFile, ReadScheduler,
    SchedulerConfig, Window, WriteFile, WriteScheduler,
};
use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::{TcpListener, TcpStream},
    time::{Sleep, sleep},
};

const BLOCK: usize = 1 << 16; // 64 KiB per frame.
const FRAMES_PER_FILE: u32 = 160; // 10 MiB per complete file.
const PIPELINE_FILES: usize = 40; // Discovery horizon and active-file limit.
const PIPELINE_FRAMES: usize = 400; // Frame budget and local buffering target.
type Frame = Vec<u8>;

struct Segment {
    path: PathBuf,
    frames: u32,
    opening: Option<Pin<Box<dyn Future<Output = io::Result<File>>>>>,
    file: Option<File>,
    offset: usize,
    finalize_delay: Option<Pin<Box<Sleep>>>,
}

impl Segment {
    fn new(path: PathBuf, frames: u32) -> Self {
        Self { path, frames, opening: None, file: None, offset: 0, finalize_delay: None }
    }
    fn poll_open_file(&mut self, cx: &mut Context<'_>, writing: bool) -> Poll<io::Result<()>> {
        if self.opening.is_none() {
            self.file = None;
            self.offset = 0;
            self.finalize_delay = None;
            let path = self.path.clone();
            self.opening = Some(Box::pin(async move {
                let delay = if writing { Duration::from_secs(1) } else { Duration::from_millis(500) };
                sleep(delay).await;
                if writing { File::create(path).await } else { File::open(path).await }
            }));
        }
        let Some(opening) = self.opening.as_mut() else {
            return Poll::Ready(Err(io::Error::other("missing open operation")));
        };
        match opening.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.opening = None;
                match result {
                    Ok(file) => { self.file = Some(file); Poll::Ready(Ok(())) },
                    Err(error) => Poll::Ready(Err(error)),
                }
            }
        }
    }
}

impl WriteFile<Frame> for Segment {
    type Error = io::Error;
    type Output = PathBuf;
    fn frame_capacity(&self) -> u32 { self.frames }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_open_file(cx, true)
    }
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, frame: &Frame) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(file) = this.file.as_mut() else { return Poll::Ready(Err(io::Error::other("file is not open"))); };
        while this.offset < frame.len() {
            let Some(bytes) = frame.get(this.offset..) else { return Poll::Ready(Err(io::Error::other("invalid write offset"))); };
            let written = ready!(Pin::new(&mut *file).poll_write(cx, bytes))?;
            if written == 0 { return Poll::Ready(Err(io::ErrorKind::WriteZero.into())); }
            let Some(offset) = this.offset.checked_add(written) else { return Poll::Ready(Err(io::Error::other("write offset overflow"))); };
            this.offset = offset;
        }
        this.offset = 0;
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<PathBuf>> {
        let this = self.get_mut();
        let Some(file) = this.file.as_mut() else { return Poll::Ready(Err(io::Error::other("file is not open"))); };
        ready!(Pin::new(file).poll_flush(cx))?;
        let delay = this.finalize_delay.get_or_insert_with(|| Box::pin(sleep(Duration::from_secs(2))));
        ready!(delay.as_mut().poll(cx));
        Poll::Ready(Ok(this.path.clone()))
    }
}

impl ReadFile<Frame> for Segment {
    type Error = io::Error;
    fn frame_count(&self) -> u32 { self.frames }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_open_file(cx, false)
    }
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, frame: &mut Frame) -> Poll<io::Result<Option<()>>> {
        let this = self.get_mut();
        let Some(file) = this.file.as_mut() else { return Poll::Ready(Err(io::Error::other("file is not open"))); };
        if this.offset == 0 && frame.len() != BLOCK { frame.resize(BLOCK, 0); }
        while this.offset < BLOCK {
            let Some(bytes) = frame.get_mut(this.offset..) else { return Poll::Ready(Err(io::Error::other("invalid read offset"))); };
            let mut buffer = ReadBuf::new(bytes);
            ready!(Pin::new(&mut *file).poll_read(cx, &mut buffer))?;
            let read = buffer.filled().len();
            if read == 0 {
                return Poll::Ready(if this.offset == 0 { Ok(None) } else { Err(io::ErrorKind::UnexpectedEof.into()) });
            }
            let Some(offset) = this.offset.checked_add(read) else { return Poll::Ready(Err(io::Error::other("read offset overflow"))); };
            this.offset = offset;
        }
        this.offset = 0;
        Poll::Ready(Ok(Some(())))
    }
}

fn config() -> SchedulerConfig {
    let window = Window::new(
        PIPELINE_FRAMES,
        PIPELINE_FILES * FRAMES_PER_FILE as usize,
        PIPELINE_FILES,
    )
    .expect("valid window");
    SchedulerConfig::new(window, 0)
}

fn next_segment(directory: &Path, next: &mut u64) -> io::Result<Segment> {
    let path = directory.join(format!("{next:020}.bin"));
    *next = next.checked_add(1).ok_or_else(|| io::Error::other("segment number overflow"))?;
    Ok(Segment::new(path, FRAMES_PER_FILE))
}

async fn write_bytes(
    writer: &mut WriteScheduler<'_, BytesFrameAllocator, Segment>,
    mut bytes: &[u8],
    directory: &Path,
    next_file: &mut u64,
) -> io::Result<()> {
    while !bytes.is_empty() {
        let status = std::future::poll_fn(|cx| Pin::new(&mut *writer).poll_write(cx, bytes))
            .await.map_err(io::Error::other)?;
        for _ in 0..status.completed_files {
            let (_, path) = writer.pop_swap(next_segment(directory, next_file)?).map_err(io::Error::other)?;
            println!("Written {}", path.display());
        }
        if let Some(consumed) = status.output {
            bytes = bytes.get(consumed..).ok_or_else(|| io::Error::other("invalid input consumption"))?;
        }
    }
    Ok(())
}

async fn upload(mut socket: TcpStream, directory: &Path) -> io::Result<u64> {
    // ponytail: one transfer at a time in this owned directory.
    fs::remove_dir_all(directory).await?;
    fs::create_dir(directory).await?;
    let budget = FrameBudget::new(PIPELINE_FRAMES);
    let mut cfg = config();
    let mut allocator = BytesFrameAllocator::new(BLOCK).map_err(io::Error::other)?;
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).map_err(io::Error::other)?;
    let mut next_file = 0;
    while writer.accept_file() {
        writer.push_file(next_segment(directory, &mut next_file)?).map_err(io::Error::other)?;
    }
    let mut input = vec![0; BLOCK];
    let mut total = 0u64;
    loop {
        let read = socket.read(&mut input).await?;
        if read == 0 { break; }
        total = total.checked_add(read as u64).ok_or_else(|| io::Error::other("input size overflow"))?;
        let bytes = input.get(..read).ok_or_else(|| io::Error::other("invalid socket read length"))?;
        write_bytes(&mut writer, bytes, directory, &mut next_file).await?;
    }
    // The on-disk format uses full blocks; stream.meta records the actual size.
    let remainder = (total % BLOCK as u64) as usize;
    if remainder != 0 {
        write_bytes(&mut writer, &vec![0; BLOCK.saturating_sub(remainder)], directory, &mut next_file).await?;
    }
    loop {
        let completed = std::future::poll_fn(|cx| Pin::new(&mut writer).poll_close(cx))
            .await.map_err(io::Error::other)?;
        if completed == 0 { break; }
        for _ in 0..completed {
            let (_, path) = writer.pop_file().map_err(io::Error::other)?
                .ok_or_else(|| io::Error::other("missing close result"))?;
            println!("Written {}", path.display());
        }
    }
    drop(writer);
    fs::write(directory.join("stream.meta"), format!("{total}\n")).await?;
    Ok(total)
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
            if size == 0 || size % BLOCK as u64 != 0 || size > FRAMES_PER_FILE as u64 * BLOCK as u64
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid segment size",
                ));
            }
            stored += size;
            files.push(Segment::new(path, (size / BLOCK as u64) as u32));
        }
    }
    if stored / BLOCK as u64 != total.div_ceil(BLOCK as u64) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segments do not match stream.meta",
        ));
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    let mut files = files.into_iter().peekable();
    let scheduler_budget_2 = FrameBudget::new(PIPELINE_FRAMES);
    let mut scheduler_config_2 = config();
    let mut reader = ReadScheduler::new(|| Vec::with_capacity(BLOCK), &scheduler_budget_2, &mut scheduler_config_2)
        .map_err(io::Error::other)?;
    let mut waiting = None;
    let mut closed = false;
    let mut remaining = total;
    let mut frame = Vec::with_capacity(BLOCK);
    while std::future::poll_fn(|cx| {
        while !closed {
            let file = match waiting.take().or_else(|| files.next()) {
                Some(file) => file,
                None => { closed = true; reader.close_files(); break; }
            };
            match reader.push_file(file) {
                Ok(()) => {},
                Err((PushError::Full, file)) => { waiting = Some(file); break; },
                Err((error, _)) => return Poll::Ready(Err(io::Error::other(error))),
            }
        }
        match reader.poll_read(cx, &mut frame) {
            Poll::Ready(result) => Poll::Ready(result.map_err(io::Error::other)),
            Poll::Pending => {
                // A read may have freed admission. Push and explicitly
                // schedule the next poll; CARBON's push does not wake itself.
                if let Some(file) = waiting.take() {
                    match reader.push_file(file) {
                        Ok(()) => cx.waker().wake_by_ref(),
                        Err((PushError::Full, file)) => waiting = Some(file),
                        Err((error, _)) => return Poll::Ready(Err(io::Error::other(error))),
                    }
                }
                Poll::Pending
            }
        }
    }).await?.is_some() {
        let length = remaining.min(BLOCK as u64) as usize;
        let bytes = frame.get(..length).ok_or_else(|| io::Error::other("short output frame"))?;
        tokio::select! {
            result = socket.write_all(bytes) => result?,
            _ = reader.progress() => return Err(io::Error::other("progress unexpectedly completed")),
        }
        remaining = remaining.saturating_sub(length as u64);
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
