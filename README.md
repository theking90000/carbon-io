# CARBON I/O

[![Crates.io](https://img.shields.io/crates/v/carbon-io.svg)](https://crates.io/crates/carbon-io)
[![Documentation](https://docs.rs/carbon-io/badge.svg)](https://docs.rs/carbon-io)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/theking90000/carbon-io/blob/main/LICENSE)
[![Rust: 1.85+](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://github.com/theking90000/carbon-io/blob/main/Cargo.toml)

**Concurrent Async Reordering Buffered Ordered Nonblocking I/O**

CARBON I/O is a Rust library for reading and writing several files concurrently
with bounded buffering and ordered output.

Each file contains frames of your chosen Rust type. CARBON schedules reads and
writes across files, limits buffering, and delivers results in order.

- `ReadScheduler` reads files into a stream of frames in file order.
- `WriteScheduler` fills frames from borrowed input, writes them to destinations,
  and returns close results in destination order.

Polling drives I/O. CARBON starts no background tasks.

Schedulers borrow a shared `&FrameBudget` and a mutable `&mut SchedulerConfig`.
Reads admit files after `file_ready().await`. Writes also borrow a frame allocator;
`poll_write()` fills its partial frame and reports completed files to replace.
The producer retains its sources and drives input and output in the same task.
Dropping a scheduler cancels its operations and releases its budget.
`set_window()` updates the caller's configuration.

See the [detailed guide](docs/guide.md) for more on scheduling, buffering,
backpressure, and backend integration.

## Hiding I/O latency

**25× the sequential throughput in this test:** 485.2 MiB/s with CARBON I/O,
versus 19.2 MiB/s sequentially. The smaller pipeline reached 178.1 MiB/s.

Test conditions: a 588 MB source file, split into 10 MiB segments of 160 frames
at 64 KiB each, with an artificial 500 ms delay per file open. The two runs
allowed 10 or 50 active files, covering 100 or 500 MiB of data ahead, with the
same 4,000-frame buffering budget, equivalent to 250 MiB. The charts' “buffer”
labels refer to that file window, not measured memory usage.

![Measured throughput compared with sequential I/O](.github/throughput-summary.png)
![A larger file window hides the simulated opening latency](.github/latency-hiding.png)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
carbon-io = "0.3.2"
futures = "0.3" # StreamExt, select! and the executor used in the examples.
```

Requires Rust 1.85 or newer. Works with any compatible async runtime.
The Tokio snippets below require Tokio.

## Reading files

Implement `ReadFile<T>` for your file descriptors. Each descriptor declares its
frame count and opens a stream that yields exactly that many successful frames,
followed by EOF.

```text
            poll_file_ready(cx)
                      │ Ready(Ok(()))
                enqueue_file(F)
                      │
                      ▼
            ┌──────────────────┐
            │  ReadScheduler   │ ── next().await ──► Frame (T)
            │     (Stream)     │
            └──────────────────┘
```

Admission follows the Sink pattern: poll readiness, then transfer ownership of
the file with `enqueue_file(F)`. On `Pending`, the producer keeps the file and waits
for a wakeup. `close_files()` ends admission; accepted files still drain in order.
`ReadScheduler` implements `Stream`; `next().await` yields a frame or an error,
and returns `None` after all accepted files have drained.

```rust
use std::{collections::VecDeque, convert::Infallible, error::Error, future::{Ready, ready}, ops::Range};
use futures::{executor::block_on, stream};
use carbon_io::{FrameBudget, ReadEvent, ReadFile, ReadScheduler, SchedulerConfig};

struct File(Range<u32>);

impl ReadFile<u32> for File {
    type Error = Infallible;
    type Reader = stream::Iter<std::vec::IntoIter<Result<u32, Infallible>>>;
    type Open = Ready<Result<Self::Reader, Self::Error>>;

    fn frame_count(&self) -> u32 { self.0.end - self.0.start }
    fn open(&self) -> Self::Open {
        ready(Ok(stream::iter(self.0.clone().map(Ok).collect::<Vec<_>>())))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let budget = FrameBudget::new(64);
        let mut config = SchedulerConfig::default();
        let mut reader = ReadScheduler::new(&budget, &mut config);
        let mut files = VecDeque::from([File(0..3), File(3..5)]);
        let mut values = Vec::new();

        while let Some(event) = reader.next_event().await? {
            match event {
                ReadEvent::FileReady => {
                    while !files.is_empty() && reader.accept_file() {
                        reader.enqueue_file(files.pop_front().unwrap())?;
                    }
                    if files.is_empty() {
                        reader.close_files()?;
                    }
                }
                ReadEvent::Frame(frame) => values.push(frame),
            }
        }
        assert_eq!(values, [0, 1, 2, 3, 4]);
        Ok(())
    })
}
```

`next_event()` handles file readiness and frame output with one mutable borrow.
Files stay in the caller's queue until admission is ready. One `FileReady` event
can admit several files: `accept_file()` reserves a place and returns `true`,
then `enqueue_file()` transfers ownership. The loop stops on backpressure;
`next_event()` drives I/O until another event is available. The same operation is
available as `poll(cx)` when implementing a custom future.

### Keep reading while the consumer waits

When handling `ReadEvent::Frame(frame)`, poll file readiness alongside the TCP
send. Once the file queue is empty, close admission and switch to `progress()`:

```ignore
let sending = write_tcp(frame);
tokio::pin!(sending);
loop {
    tokio::select! {
        result = &mut sending => {
            result?;
            break;
        }
        ready = async {
            if files.is_empty() {
                reader.close_files()?;
                reader.progress().await;
            }
            reader.file_ready().await
        } => {
            ready?;
            while !files.is_empty() && reader.accept_file() {
                reader.enqueue_file(files.pop_front().unwrap())?;
            }
            if files.is_empty() {
                reader.close_files()?;
            }
        }
    }
}
```

`file_ready()` drives I/O without consuming frames. `progress()` does the same
after admission closes and never completes. The pinned send survives each loop
turn, so a successful admission does not cancel a partially completed TCP write.

## Writing files

Implement `WriteFile<T>` for a destination that opens, writes borrowed frames,
and closes on the same object. A `FrameAllocator` creates empty partial frames;
`PartialFrame` fills them from borrowed input. `BytesFrameAllocator` provides
fixed-size byte frames.

Fill the destination window with `push_file()`. Each `poll_write()` reports input
consumption, completed files at the head, and missing descriptors. Replace
completed heads with `pop_swap()` to recover their descriptors and close results
while appending new destinations. When input ends, use `poll_close()` and
`pop_file()` to drain the remaining results without supplying replacements.

```rust
use std::{convert::Infallible, error::Error, future::poll_fn, pin::Pin, task::{Context, Poll}};
use futures::executor::block_on;
use carbon_io::{AsyncFilesWrite, BytesFrameAllocator, FrameBudget, SchedulerConfig, WriteFile, WriteScheduler};

#[derive(Default)]
struct File(Vec<u8>);
impl WriteFile<Vec<u8>> for File {
    type Error = Infallible;
    type Output = Vec<u8>;
    fn frame_capacity(&self) -> u32 { 2 }
    fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Infallible>> {
        self.get_mut().0.clear();
        Poll::Ready(Ok(()))
    }
    fn poll_write(self: Pin<&mut Self>, _: &Context<'_>, frame: &Vec<u8>)
        -> Poll<Result<(), Infallible>>
    {
        self.get_mut().0.extend_from_slice(frame);
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<Vec<u8>, Infallible>> {
        Poll::Ready(Ok(std::mem::take(&mut self.get_mut().0)))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let budget = FrameBudget::new(64);
        let mut config = SchedulerConfig::default();
        config.window.max_active_files = 1;
        let mut allocator = BytesFrameAllocator::new(3)?;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config)?;
        writer.push_file(File::default())?;
        let mut input = &b"abcdefg"[..];
        let mut results = Vec::new();
        while !input.is_empty() {
            let status = poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, input)).await?;
            for _ in 0..status.completed_files {
                let (_, result) = writer.pop_swap(File::default())?;
                results.push(result);
            }
            if let Some(consumed) = status.output { input = &input[consumed..]; }
        }
        loop {
            let completed = poll_fn(|cx| Pin::new(&mut writer).poll_close(cx)).await?;
            if completed == 0 { break; }
            for _ in 0..completed {
                results.push(writer.pop_file()?.unwrap().1);
            }
        }
        assert_eq!(results, [b"abcdef".to_vec(), b"g".to_vec()]);
        Ok(())
    })
}
```

Input frames fill destinations in order, up to each destination's capacity.
Destinations open only after receiving a frame. Closing flushes a nonempty
partial frame; empty input opens no file. If `poll_write()` returns `Pending`,
input remains unconsumed and the supplied waker signals when polling can resume.

## Choosing the window and budget

`SchedulerConfig` holds a `Window` and a retry count. The window determines how
far the scheduler may work ahead of the consumer:

| Window field | Default | Controls |
| --- | ---: | --- |
| `target_frames` | 256 | Desired local frame capacity |
| `open_ahead_frames` | 1024 | How far ahead, in frames, files may be discovered |
| `max_active_files` | 16 | Maximum active files, including unopened write destinations |

`FrameBudget` limits capacity across schedulers. Clone it to share one budget.
It counts scheduler-owned frames and reservations, not bytes, backend buffers,
frames already handed to the consumer, or completed write results.

When a writer pulls from a reader sharing the same budget, leave enough capacity
for both to progress, or the pipeline can deadlock.

Use `set_window(window)` to change either scheduler's window. Shrinking preserves
already authorized frames and opened files, then takes effect as work drains.
Allocated buffer storage does not shrink.

Frame counts, destination capacities, the global budget, `target_frames`, and
`max_active_files` must be nonzero. Invalid configurations and file contracts
are reported by scheduler operations.

## Retries, errors, and cancellation

`max_retries` defaults to zero and counts additional attempts per file. Reads
retry opening failures only. A reader error, early EOF, or extra frame is fatal.
Writes can retry opening, writing, or finalizing, with one shared retry count
across those stages.

Write retries change how long frames must be retained:

| Retry setting | Frame lifetime | Capacity requirement |
| --- | --- | --- |
| `max_retries == 0` | Drop each frame after a successful `poll_write` | A file may exceed the global frame budget |
| `max_retries > 0` | Retain the whole file until successful finalization | Reserve the whole file before accepting its first frame; it must fit the global budget |

A write retry reopens the same file object and replays from the
first retained frame. Reservations may exceed `target_frames` to fit one file.
There is no global rollback if an earlier file fails.

A fatal error releases scheduler resources immediately. Reads emit the error
once, then return EOF. Writes return the error from the current poll and reject
later operations with `InputClosed`.

Dropping a pending `next()` or a `progress()` future preserves the scheduler's
state. Dropping the scheduler abandons its operations and releases its resources.
It does not undo writes. Backends must clean up abandoned attempts.

## Integrating a backend or custom stream

`ReadScheduler` implements `Stream`. Its entry points are:

| Entry point | Behavior |
| --- | --- |
| `next().await` / `poll_next(cx)` | Remove the next ordered output, advancing I/O if needed |
| `next_event().await` / `poll(cx)` | Return input readiness or the next ordered output with one mutable borrow |
| `next_event_with_interest(interest).await` / `poll_with_interest(cx, interest)` | Return only requested event kinds, errors or EOF |
| `file_ready().await` | Reserve one input admission without consuming output |
| `accept_file()` | Reserve one admission synchronously, returning a bool without taking an item or polling backend I/O |
| `enqueue_file(file)` | Transfer ownership into a reserved admission |
| `progress()` | Keep advancing I/O without consuming output; never completes |
| `poll_progress(cx)` | Advance at most 256 work items from a custom future or stream |

Backends must follow these rules:

- Register the supplied waker before returning `Pending`.
- For reads, make every `open()` an independent attempt that can be abandoned by dropping it.
- For writes, reopen the same file object after an error and cancel its previous attempt.
- A pending `poll_write` receives the same logical frame on its next poll, but
  its address may change. Do not retain the borrowed `&T` after returning.
- Finalize only after all assigned writes succeed.

Frames need neither `Clone`, `Arc`, `Send`, nor `Unpin`. Read opening futures and
readers may be `!Unpin`. Write files must be `Unpin` or supplied through a pinned
pointer. Partial frames and borrowed write input need neither `Clone` nor `Unpin`.
The crate contains no unsafe code.

Rejected admissions return contract errors; rejected write calls preserve accepted work.
`FramePermit::poll_grow` returns `Poll<Result<(), ContractError>>`; an invalid
growth request returns `InvalidBudgetRequest` and preserves existing grants or
waits.

## Examples and performance

Each example is an independent crate with its own dependencies and lockfile.
See the [examples guide](examples/README.md) for the memory, TCP/file, Reqwest,
and Pingora uploads and their launch commands.

The [memory example](https://github.com/theking90000/carbon-io/blob/main/examples/memory/src/main.rs)
reads frames, then writes them and collects file close results:

```sh
cargo run --locked --manifest-path examples/memory/Cargo.toml
```

See the [benchmark report](https://github.com/theking90000/carbon-io/blob/main/docs/benchmarks.md)
for in-memory throughput, backend polls, and allocations.

## Development

```sh
cargo test --locked
cargo test --locked --release
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo bench --locked --bench scheduler
```

The [original specification](https://github.com/theking90000/carbon-io/blob/main/docs/specification.md) contains the French CDC and its
V1 amendments.

MIT licensed.
