//! Read 20 MiB files into reusable 64 KiB frames with a 16 MiB buffer window.
use carbon_io::{FrameBudget, PushError, ReadFile, ReadScheduler, SchedulerConfig, Window};
use futures::task::noop_waker;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{
    alloc::System,
    convert::Infallible,
    error::Error,
    hint::black_box,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const FRAME_BYTES: usize = 64 * 1024;
const FRAMES_PER_FILE: u32 = 320;
const FILES: usize = 100;
const TARGET: usize = 256;

struct File {
    next: usize,
}

impl ReadFile<Vec<u8>> for File {
    type Error = Infallible;

    fn frame_count(&self) -> u32 {
        FRAMES_PER_FILE
    }

    fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &mut Vec<u8>,
    ) -> Poll<Result<Option<()>, Self::Error>> {
        let this = self.get_mut();
        frame.resize(FRAME_BYTES, 0);
        frame.fill(this.next as u8);
        this.next = this.next.wrapping_add(1);
        Poll::Ready(Ok(Some(())))
    }
}

fn run(active: usize) -> Result<(), Box<dyn Error>> {
    let region = Region::new(ALLOCATOR);
    let start = Instant::now();
    let budget = FrameBudget::new(TARGET);
    let mut config = SchedulerConfig::new(Window::new(TARGET, 8000, active)?, 0);
    let mut reader = ReadScheduler::new(|| Vec::with_capacity(FRAME_BYTES), &budget, &mut config)?;
    let mut files = (0..FILES).map(|i| File {
        next: i.saturating_mul(FRAMES_PER_FILE as usize),
    });
    let mut waiting = None;
    let mut closed = false;
    let mut output = Vec::with_capacity(FRAME_BYTES);
    let mut frames = 0_usize;
    let mut checksum = 0_u64;
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        while !closed {
            let Some(file) = waiting.take().or_else(|| files.next()) else {
                closed = true;
                reader.close_files();
                break;
            };
            match reader.push_file(file) {
                Ok(()) => {}
                Err((PushError::Full, file)) => {
                    waiting = Some(file);
                    break;
                }
                Err((error, _)) => return Err(error.into()),
            }
        }
        match reader.poll_read(&mut cx, &mut output) {
            Poll::Ready(Ok(Some(()))) => {
                let payload = black_box(output.as_slice());
                let expected = frames as u8;
                if payload.len() != FRAME_BYTES
                    || payload.first() != Some(&expected)
                    || payload.last() != Some(&expected)
                {
                    return Err(io::Error::other("invalid frame length or order").into());
                }
                checksum = checksum.wrapping_add(u64::from(expected));
                frames = frames.saturating_add(1);
            }
            Poll::Ready(Ok(None)) => break,
            Poll::Ready(Err(error)) => return Err(error.into()),
            Poll::Pending => {}
        }
    }

    let elapsed = start.elapsed();
    let allocations = region.change();
    let total = FILES.saturating_mul(FRAMES_PER_FILE as usize);
    let expected_sum = (0..total).fold(0_u64, |sum, i| sum.wrapping_add(u64::from(i as u8)));
    if frames != total || checksum != expected_sum {
        return Err(io::Error::other("invalid frame count or checksum").into());
    }
    let seconds = elapsed.as_secs_f64();
    let gib = frames as f64 * FRAME_BYTES as f64 / (1024.0 * 1024.0 * 1024.0);
    println!(
        "{active},{FILES},{frames},{FRAME_BYTES},{TARGET},{:.3},{:.0},{:.2},{},{}",
        seconds * 1000.0,
        frames as f64 / seconds,
        gib / seconds,
        allocations.allocations + allocations.reallocations,
        allocations.bytes_allocated + allocations.bytes_reallocated.max(0) as usize,
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // ponytail: synchronous memory fills isolate buffer and scheduler costs;
    // measure backend latency separately when using real storage.
    println!(
        "max_active_files,files,frames,frame_bytes,target_frames,elapsed_ms,frames_per_second,gib_per_second,allocations,allocated_bytes"
    );
    run(1)?;
    run(25)
}
