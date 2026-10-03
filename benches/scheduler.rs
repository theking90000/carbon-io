//! Reproducible throughput, polling and allocation measurements without a harness.
use carbon_io::{
    FrameBudget, FrameWriter, ReadFile, ReadScheduler, SchedulerConfig, Window, WriteFile,
    WriteScheduler,
};
use futures::{FutureExt, Stream, StreamExt, stream, task::noop_waker};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{
    alloc::System,
    cell::Cell,
    future::{Future, Ready, ready},
    hint::black_box,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Instant,
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

#[derive(Clone)]
struct ReadSource {
    start: u64,
    count: u32,
    pending: bool,
    polls: Rc<Cell<usize>>,
}
struct Reader {
    next: u64,
    end: u64,
    pending: bool,
    waiting: bool,
    polls: Rc<Cell<usize>>,
}
struct Opening {
    source: Option<ReadSource>,
    waiting: bool,
}
impl Future for Opening {
    type Output = Result<Reader, ()>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let source = this.source.as_ref().unwrap();
        source.polls.set(source.polls.get() + 1);
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let source = this.source.take().unwrap();
        Poll::Ready(Ok(Reader {
            next: source.start,
            end: source.start + u64::from(source.count),
            pending: source.pending,
            waiting: source.pending,
            polls: source.polls,
        }))
    }
}
impl ReadFile<u64> for ReadSource {
    type Error = ();
    type Open = Opening;
    type Reader = Reader;
    fn frame_count(&self) -> u32 {
        self.count
    }
    fn open(&self) -> Self::Open {
        Opening {
            source: Some(self.clone()),
            waiting: self.pending,
        }
    }
}
impl Stream for Reader {
    type Item = Result<u64, ()>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if this.next == this.end {
            return Poll::Ready(None);
        }
        let value = this.next;
        this.next += 1;
        this.waiting = this.pending;
        Poll::Ready(Some(Ok(value)))
    }
}
struct Destination {
    count: u32,
    pending: bool,
    retry: bool,
    attempts: Cell<usize>,
    polls: Rc<Cell<usize>>,
}
struct Writer {
    sum: u64,
    pending: bool,
    waiting: bool,
    fail: bool,
    polls: Rc<Cell<usize>>,
}
impl WriteFile<u64> for Destination {
    type Error = ();
    type Output = u64;
    type Open = Ready<Result<Writer, ()>>;
    type Writer = Writer;
    fn frame_capacity(&self) -> u32 {
        self.count
    }
    fn open(&self) -> Self::Open {
        let attempt = self.attempts.get();
        self.attempts.set(attempt + 1);
        ready(Ok(Writer {
            sum: 0,
            pending: self.pending,
            waiting: self.pending,
            fail: self.retry && attempt == 0,
            polls: self.polls.clone(),
        }))
    }
}
impl FrameWriter<u64> for Writer {
    type Error = ();
    type Output = u64;
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, frame: &u64) -> Poll<Result<(), ()>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.sum += *frame;
        this.waiting = this.pending;
        Poll::Ready(Ok(()))
    }
    fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u64, ()>> {
        self.polls.set(self.polls.get() + 1);
        Poll::Ready(if self.fail { Err(()) } else { Ok(self.sum) })
    }
}
fn config(target: usize, active: usize, retries: u32) -> SchedulerConfig {
    let mut window = Window::default();
    window.target_frames = target;
    window.open_ahead_frames = target * 2;
    window.max_active_files = active;
    SchedulerConfig::new(window, retries)
}
fn consume<S: Stream<Item = Result<u64, E>> + Unpin, E: std::fmt::Debug>(mut stream: S) -> u64 {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut sum = 0;
    loop {
        match Pin::new(&mut stream).poll_next(&mut cx) {
            Poll::Ready(Some(value)) => sum += black_box(value.unwrap()),
            Poll::Ready(None) => return sum,
            Poll::Pending => {}
        }
    }
}
fn measure(
    name: &str,
    frames: usize,
    files: usize,
    polls: Rc<Cell<usize>>,
    run: impl FnOnce() -> u64,
) {
    let region = Region::new(ALLOCATOR);
    let start = Instant::now();
    let sum = run();
    let elapsed = start.elapsed();
    let allocations = region.change();
    let expected = (frames as u64) * (frames as u64 - 1) / 2;
    assert_eq!(sum, expected);
    println!(
        "{name},{frames},{files},{:.0},{:.3},{:.6},{:.3},{},{}",
        frames as f64 / elapsed.as_secs_f64(),
        polls.get() as f64 / frames as f64,
        (allocations.allocations + allocations.reallocations) as f64 / frames as f64,
        (allocations.allocations + allocations.reallocations) as f64 / files as f64,
        allocations.bytes_allocated + allocations.bytes_reallocated.max(0) as usize,
        elapsed.as_nanos()
    );
}
fn read_case(name: &str, n: usize, size: u32, active: usize, pending: bool) {
    let polls = Rc::new(Cell::new(0));
    let count = n * size as usize;
    let target = (active * size as usize).clamp(1, 4096);
    measure(name, count, n, polls.clone(), || {
        let mut files = (0..n).map(|i| ReadSource {
            start: (i * size as usize) as u64,
            count: size,
            pending,
            polls: polls.clone(),
        });
        let scheduler_budget_1 = FrameBudget::new(target);
        let mut scheduler_config_1 = config(target, active, 0);
        let mut reader = ReadScheduler::new(&scheduler_budget_1, &mut scheduler_config_1);
        let mut closed = false;
        consume(stream::poll_fn(|cx| {
            while !closed {
                match Pin::new(&mut reader).poll_file_ready(cx) {
                    Poll::Pending => break,
                    Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                    Poll::Ready(Ok(())) => match files.next() {
                        Some(file) => {
                            if let Err(error) = Pin::new(&mut reader).enqueue_file(file) {
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                        None => {
                            closed = true;
                            if let Err(error) = Pin::new(&mut reader).close_files() {
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                    },
                }
            }
            Pin::new(&mut reader).poll_next(cx)
        }))
    })
}
// Feed benchmark inputs from the calling task, including during consumer waits.
fn feed_writer(
    writer: &mut WriteScheduler<'_, u64, Destination>,
    files: &mut impl Iterator<Item = Destination>,
    next: &mut u64,
    total: u64,
    files_closed: &mut bool,
    cx: &mut Context<'_>,
) -> Result<(), carbon_io::SchedulerError<()>> {
    for turn in 0..64 {
        let mut advanced = false;
        if !*files_closed && Pin::new(&mut *writer).poll_file_ready(cx)?.is_ready() {
            match files.next() {
                Some(file) => Pin::new(&mut *writer).enqueue_file(file)?,
                None => {
                    Pin::new(&mut *writer).close_files()?;
                    *files_closed = true;
                }
            }
            advanced = true;
        }
        if *next < total && Pin::new(&mut *writer).poll_frame_ready(cx)?.is_ready() {
            Pin::new(&mut *writer).enqueue_frame(*next)?;
            *next += 1;
            advanced = true;
            if *next == total {
                Pin::new(&mut *writer).close_frames()?;
                Pin::new(&mut *writer).close_files()?;
                *files_closed = true;
            }
        }
        if !advanced {
            break;
        }
        if turn == 63 {
            cx.waker().wake_by_ref();
        }
    }
    Ok(())
}

fn write_case(name: &str, n: usize, size: u32, active: usize, pending: bool, retry: bool) {
    let polls = Rc::new(Cell::new(0));
    let count = n * size as usize;
    let target = (active * size as usize).clamp(1, 4096);
    let budget = target.max(if retry { size as usize } else { 1 });
    measure(name, count, n, polls.clone(), || {
        let mut files = (0..n).map(|_| Destination {
            count: size,
            pending,
            retry,
            attempts: Cell::new(0),
            polls: polls.clone(),
        });
        let scheduler_budget_2 = FrameBudget::new(budget);
        let mut scheduler_config_2 = config(target, active, u32::from(retry));
        let mut writer = WriteScheduler::new(&scheduler_budget_2, &mut scheduler_config_2);
        let mut next = 0;
        let mut files_closed = false;
        consume(stream::poll_fn(|cx| {
            if let Err(error) = feed_writer(
                &mut writer,
                &mut files,
                &mut next,
                count as u64,
                &mut files_closed,
                cx,
            ) {
                return Poll::Ready(Some(Err(error)));
            }
            Pin::new(&mut writer).poll_next(cx)
        }))
    })
}
fn shared_case(n: usize) {
    let polls = Rc::new(Cell::new(0));
    measure("shared_four_schedulers", n * 4, 4, polls.clone(), || {
        let budget = FrameBudget::new(4096);
        let files = (0..4).map(|i| ReadSource {
            start: (i * n) as u64,
            count: n as u32,
            pending: false,
            polls: polls.clone(),
        });
        let mut configs = [config(1024, 1, 0); 4];
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut schedulers: Vec<_> = files
            .zip(&mut configs)
            .map(|(file, config)| {
                let mut reader = ReadScheduler::new(&budget, config);
                assert!(matches!(
                    Pin::new(&mut reader).poll_file_ready(&mut cx),
                    Poll::Ready(Ok(()))
                ));
                Pin::new(&mut reader).enqueue_file(file).unwrap();
                Pin::new(&mut reader).close_files().unwrap();
                reader
            })
            .collect();
        let mut sum = 0;
        let mut finished = [false; 4];
        while finished.iter().any(|done| !*done) {
            for (i, s) in schedulers.iter_mut().enumerate() {
                if !finished[i] {
                    match Pin::new(s).poll_next(&mut cx) {
                        Poll::Ready(Some(value)) => sum += black_box(value.unwrap()),
                        Poll::Ready(None) => finished[i] = true,
                        Poll::Pending => {}
                    }
                }
            }
        }
        sum
    });
}
// Four cooperative consumer waits per output, with no clock or I/O latency.
async fn consumer_wait() {
    let mut remaining = 4;
    std::future::poll_fn(|cx| {
        if remaining == 0 {
            Poll::Ready(())
        } else {
            remaining -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

fn read_consumer_case(name: &str, count: u32, drive: bool) {
    let polls = Rc::new(Cell::new(0));
    measure(name, count as usize, 1, polls.clone(), || {
        futures::executor::block_on(async {
            let file = ReadSource {
                start: 0,
                count,
                pending: true,
                polls,
            };
            let scheduler_budget_3 = FrameBudget::new(64);
            let mut scheduler_config_3 = config(64, 1, 0);
            let mut reader = ReadScheduler::new(&scheduler_budget_3, &mut scheduler_config_3);
            std::future::poll_fn(|cx| Pin::new(&mut reader).poll_file_ready(cx))
                .await
                .unwrap();
            Pin::new(&mut reader).enqueue_file(file).unwrap();
            Pin::new(&mut reader).close_files().unwrap();
            let mut sum = 0;
            while let Some(frame) = reader.next().await {
                sum += black_box(frame.unwrap());
                if drive {
                    futures::select_biased! {
                        _ = reader.progress().fuse() => unreachable!(),
                        _ = consumer_wait().fuse() => {},
                    }
                } else {
                    consumer_wait().await;
                }
            }
            sum
        })
    });
}

fn write_consumer_case(name: &str, files: usize, drive: bool) {
    let polls = Rc::new(Cell::new(0));
    let count = files * 4;
    measure(name, count, files, polls.clone(), || {
        futures::executor::block_on(async {
            let mut destinations = (0..files).map(|_| Destination {
                count: 4,
                pending: true,
                retry: false,
                attempts: Cell::new(0),
                polls: polls.clone(),
            });
            let scheduler_budget_4 = FrameBudget::new(64);
            let mut scheduler_config_4 = config(64, 16, 0);
            let mut writer = WriteScheduler::new(&scheduler_budget_4, &mut scheduler_config_4);
            let mut next = 0;
            let mut files_closed = false;
            let mut sum = 0;
            while let Some(result) = std::future::poll_fn(|cx| {
                if let Err(error) = feed_writer(
                    &mut writer,
                    &mut destinations,
                    &mut next,
                    count as u64,
                    &mut files_closed,
                    cx,
                ) {
                    return Poll::Ready(Some(Err(error)));
                }
                Pin::new(&mut writer).poll_next(cx)
            })
            .await
            {
                sum += black_box(result.unwrap());
                if drive {
                    futures::select_biased! {
                        _ = std::future::poll_fn(|cx| {
                            feed_writer(
                                &mut writer,
                                &mut destinations,
                                &mut next,
                                count as u64,
                                &mut files_closed,
                                cx,
                            ).unwrap();
                            writer.poll_progress(cx);
                            Poll::<()>::Pending
                        }).fuse() => unreachable!(),
                        _ = consumer_wait().fuse() => {},
                    }
                } else {
                    consumer_wait().await;
                }
            }
            sum
        })
    });
}

fn main() {
    let scale = std::env::var("CARBON_IO_BENCH_SCALE")
        .ok()
        .map(|s| s.parse::<usize>().expect("positive integer scale"))
        .unwrap_or(1)
        .max(1);
    println!(
        "case,frames,files,frames_per_second,backend_polls_per_frame,allocations_per_frame,allocations_per_file,allocated_bytes,elapsed_ns"
    );
    let baseline_polls = Rc::new(Cell::new(0));
    measure(
        "raw_stream_baseline",
        100_000 * scale,
        1,
        baseline_polls.clone(),
        || {
            consume(stream::iter(0..(100_000 * scale) as u64).map(|value| {
                baseline_polls.set(baseline_polls.get() + 1);
                Ok::<_, ()>(value)
            }))
        },
    );
    read_case("read_one_large_file", 1, 100_000 * scale as u32, 1, false);
    read_case("read_many_small_files", 10_000 * scale, 4, 64, false);
    read_case("read_multiple_active", 100 * scale, 1024, 32, false);
    read_case("read_backpressure", 50 * scale, 256, 16, true);
    read_case("read_many_openings", 10_000 * scale, 1, 10_000, true);
    write_case(
        "write_one_large_file",
        1,
        100_000 * scale as u32,
        1,
        false,
        false,
    );
    write_case(
        "write_many_small_files",
        10_000 * scale,
        4,
        64,
        false,
        false,
    );
    write_case("write_multiple_active", 100 * scale, 1024, 32, false, false);
    write_case("write_backpressure", 50 * scale, 256, 16, true, false);
    write_case("write_finalize_retry", 100 * scale, 1024, 32, false, true);
    shared_case(25_000 * scale);
    // Initialize the executor outside allocation measurements for both variants.
    futures::executor::block_on(ready(()));
    read_consumer_case("read_consumer_wait", 8192 * scale as u32, false);
    read_consumer_case("read_consumer_drive", 8192 * scale as u32, true);
    write_consumer_case("write_consumer_wait", 2048 * scale, false);
    write_consumer_case("write_consumer_drive", 2048 * scale, true);
}
