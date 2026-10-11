//! Reproducible throughput, polling and allocation measurements without a harness.
use carbon_io::{
    AsyncFilesWrite, FrameAllocator, FrameBudget, PartialFrame, ReadFile, ReadScheduler,
    SchedulerConfig, Window, WriteFile, WriteScheduler,
};
use futures::{FutureExt, Stream, StreamExt, stream, task::noop_waker};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};
use std::{
    alloc::System,
    cell::Cell,
    future::ready,
    hint::black_box,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Instant,
};

#[global_allocator]
static ALLOCATOR: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

struct ReadSource {
    start: u64,
    count: u32,
    pending: bool,
    polls: Rc<Cell<usize>>,
    opening_wait: bool,
    reader: Option<Reader>,
}
struct Reader {
    next: u64,
    end: u64,
    pending: bool,
    waiting: bool,
    polls: Rc<Cell<usize>>,
}
impl ReadFile<u64> for ReadSource {
    type Error = ();
    fn frame_count(&self) -> u32 {
        self.count
    }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        if this.opening_wait && this.pending {
            this.opening_wait = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.reader = Some(Reader {
            next: this.start,
            end: this.start.saturating_add(u64::from(this.count)),
            pending: this.pending,
            waiting: this.pending,
            polls: this.polls.clone(),
        });
        Poll::Ready(Ok(()))
    }
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &mut u64,
    ) -> Poll<Result<Option<()>, ()>> {
        let Some(reader) = self.get_mut().reader.as_mut() else {
            return Poll::Ready(Err(()));
        };
        reader.polls.set(reader.polls.get() + 1);
        if reader.waiting {
            reader.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if reader.next == reader.end {
            return Poll::Ready(Ok(None));
        }
        *frame = reader.next;
        reader.next = reader.next.saturating_add(1);
        reader.waiting = reader.pending;
        Poll::Ready(Ok(Some(())))
    }
}
struct Destination {
    count: u32,
    pending: bool,
    retry: bool,
    attempts: Cell<usize>,
    writer: Option<Writer>,
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
    fn frame_capacity(&self) -> u32 {
        self.count
    }
    fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
        let this = self.get_mut();
        let attempt = this.attempts.get();
        this.attempts.set(attempt + 1);
        this.writer = Some(Writer {
            sum: 0,
            pending: this.pending,
            waiting: this.pending,
            fail: this.retry && attempt == 0,
            polls: this.polls.clone(),
        });
        Poll::Ready(Ok(()))
    }
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, frame: &u64) -> Poll<Result<(), ()>> {
        let this = self.get_mut().writer.as_mut().unwrap();
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
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u64, ()>> {
        let this = self.get_mut().writer.as_ref().unwrap();
        this.polls.set(this.polls.get() + 1);
        Poll::Ready(if this.fail { Err(()) } else { Ok(this.sum) })
    }
}
struct Allocator;
struct Partial(Option<u64>);
impl FrameAllocator for Allocator {
    type Partial = Partial;
    fn allocate(&mut self) -> Partial {
        Partial(None)
    }
}
impl PartialFrame for Partial {
    type Input = u64;
    type Output = ();
    type Frame = u64;
    type Error = std::convert::Infallible;
    fn poll_fill(&mut self, _: &Context<'_>, input: &u64) -> Poll<Result<(), Self::Error>> {
        self.0 = Some(*input);
        Poll::Ready(Ok(()))
    }
    fn is_complete(&self) -> bool {
        self.0.is_some()
    }
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    fn finish(self) -> u64 {
        self.0.unwrap()
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
            opening_wait: true,
            reader: None,
        });
        let budget = FrameBudget::new(target);
        let mut cfg = config(target, active, 0);
        let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
        let mut waiting = None;
        let mut closed = false;
        let mut output = 0;
        let mut sum = 0;
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        loop {
            while !closed {
                let file = match waiting.take().or_else(|| files.next()) {
                    Some(file) => file,
                    None => {
                        closed = true;
                        reader.close_files();
                        break;
                    }
                };
                match reader.push_file(file) {
                    Ok(()) => {}
                    Err((carbon_io::PushError::Full, file)) => {
                        waiting = Some(file);
                        break;
                    }
                    Err((error, _)) => panic!("{error}"),
                }
            }
            match reader.poll_read(&mut cx, &mut output) {
                Poll::Ready(Ok(Some(()))) => sum += black_box(output),
                Poll::Ready(Ok(None)) => return sum,
                Poll::Ready(Err(error)) => panic!("{error:?}"),
                Poll::Pending => {}
            }
        }
    })
}
// The caller owns input production, FIFO replacement, and close-result consumption.
fn poll_writer(
    writer: &mut WriteScheduler<'_, Allocator, Destination>,
    files: &mut impl Iterator<Item = Destination>,
    next: &mut u64,
    total: u64,
    batch: usize,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<u64, carbon_io::WriteError<std::convert::Infallible, ()>>>> {
    while writer.accept_file() {
        writer.push_file(files.next().unwrap())?;
    }
    for _ in 0..batch {
        if *next == total {
            return match Pin::new(&mut *writer).poll_close(cx)? {
                Poll::Pending => Poll::Pending,
                Poll::Ready(0) => Poll::Ready(None),
                Poll::Ready(_) => Poll::Ready(Some(Ok(writer.pop_file()?.unwrap().1))),
            };
        }
        let status = match Pin::new(&mut *writer).poll_write(cx, &black_box(*next))? {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(status) => status,
        };
        if status.output.is_some() {
            *next += 1;
        }
        if status.completed_files > 0 {
            return Poll::Ready(Some(Ok(writer.pop_swap(files.next().unwrap())?.1)));
        }
    }
    cx.waker().wake_by_ref();
    Poll::Pending
}

fn write_case(name: &str, n: usize, size: u32, active: usize, pending: bool, retry: bool) {
    let polls = Rc::new(Cell::new(0));
    let count = n * size as usize;
    let target = (active * size as usize).clamp(1, 4096);
    let budget = target.max(if retry { size as usize } else { 1 });
    measure(name, count, n, polls.clone(), || {
        let mut files = (0..).map(|_| Destination {
            count: size,
            pending,
            retry,
            attempts: Cell::new(0),
            writer: None,
            polls: polls.clone(),
        });
        let scheduler_budget_2 = FrameBudget::new(budget);
        let mut scheduler_config_2 = config(target, active, u32::from(retry));
        let mut allocator = Allocator;
        let mut writer =
            WriteScheduler::new(&mut allocator, &scheduler_budget_2, &mut scheduler_config_2)
                .unwrap();
        let mut next = 0;
        consume(stream::poll_fn(|cx| {
            poll_writer(&mut writer, &mut files, &mut next, count as u64, 64, cx)
        }))
    })
}
fn write_poll_case(name: &str, count: usize, batch: usize) {
    let polls = Rc::new(Cell::new(0));
    measure(name, count, 1, polls.clone(), || {
        let budget = FrameBudget::new(64);
        let mut cfg = config(64, 1, 0);
        let mut allocator = Allocator;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        let mut files = (0..).map(|_| Destination {
            count: count as u32,
            pending: false,
            retry: false,
            attempts: Cell::new(0),
            polls: polls.clone(),
            writer: None,
        });
        let mut next = 0;
        consume(stream::poll_fn(|cx| {
            poll_writer(&mut writer, &mut files, &mut next, count as u64, batch, cx)
        }))
    });
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
            opening_wait: true,
            reader: None,
        });
        let mut configs = [config(1024, 1, 0); 4];
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut schedulers: Vec<_> = files
            .zip(&mut configs)
            .map(|(file, config)| {
                let mut reader = ReadScheduler::new(|| 0, &budget, config).unwrap();
                assert!(reader.push_file(file).is_ok());
                reader.close_files();
                reader
            })
            .collect();
        let mut sum = 0;
        let mut finished = [false; 4];
        while finished.iter().any(|done| !*done) {
            for (i, s) in schedulers.iter_mut().enumerate() {
                if !finished[i] {
                    let mut output = 0;
                    match s.poll_read(&mut cx, &mut output) {
                        Poll::Ready(Ok(Some(()))) => sum += black_box(output),
                        Poll::Ready(Ok(None)) => finished[i] = true,
                        Poll::Ready(Err(error)) => panic!("{error:?}"),
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
                opening_wait: true,
                reader: None,
            };
            let scheduler_budget_3 = FrameBudget::new(64);
            let mut scheduler_config_3 = config(64, 1, 0);
            let mut reader =
                ReadScheduler::new(|| 0, &scheduler_budget_3, &mut scheduler_config_3).unwrap();
            assert!(reader.push_file(file).is_ok());
            reader.close_files();
            let mut sum = 0;
            let mut frame = 0;
            while std::future::poll_fn(|cx| reader.poll_read(cx, &mut frame))
                .await
                .unwrap()
                .is_some()
            {
                sum += black_box(frame);
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
            let mut destinations = (0..).map(|_| Destination {
                count: 4,
                pending: true,
                retry: false,
                attempts: Cell::new(0),
                polls: polls.clone(),
                writer: None,
            });
            let budget = FrameBudget::new(64);
            let mut cfg = config(64, 16, 0);
            let mut allocator = Allocator;
            let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
            let mut next = 0;
            let mut sum = 0;
            while let Some(result) = std::future::poll_fn(|cx| {
                poll_writer(
                    &mut writer,
                    &mut destinations,
                    &mut next,
                    count as u64,
                    64,
                    cx,
                )
            })
            .await
            {
                sum += black_box(result.unwrap());
                if drive {
                    futures::select_biased! {
                        _ = std::future::poll_fn(|cx| {
                            if let Poll::Ready(Some(result)) = poll_writer(&mut writer,&mut destinations,&mut next,count as u64,64,cx) {
                                sum += black_box(result.unwrap());
                                cx.waker().wake_by_ref();
                            }
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
    read_case("read_active_25", 100 * scale, 1024, 25, false);
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
    write_poll_case("write_poll_per_frame", 100_000 * scale, 1);
    write_poll_case("write_poll_batched", 100_000 * scale, usize::MAX);
    shared_case(25_000 * scale);
    // Initialize the executor outside allocation measurements for both variants.
    futures::executor::block_on(ready(()));
    read_consumer_case("read_consumer_wait", 8192 * scale as u32, false);
    read_consumer_case("read_consumer_drive", 8192 * scale as u32, true);
    write_consumer_case("write_consumer_wait", 2048 * scale, false);
    write_consumer_case("write_consumer_drive", 2048 * scale, true);
}
