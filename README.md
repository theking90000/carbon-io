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
- `WriteScheduler` distributes admitted frames across destinations and returns
  their finalization results in destination order.

Consuming the stream drives I/O. CARBON starts no background tasks.

Schedulers borrow a shared `&FrameBudget` and a mutable `&mut SchedulerConfig`.
Files are supplied with `file_ready().await` followed by `enqueue_file()`;
`WriteScheduler` also accepts owned frames with `frame_ready().await` followed by
`enqueue_frame()`. `next_event().await` combines readiness and output in one borrow.
After a readiness event, `accept_file()` and `accept_frame()` reserve further
admissions without polling I/O again. The producer retains its sources and drives
admission and output in the same task. Dropping a scheduler cancels its operations and releases its
budget. `set_window()` updates the caller's configuration.

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

Implement `WriteFile<T>` to describe a destination and `FrameWriter<T>` to write
its frames. A destination declares its frame capacity. Its writer accepts frames
by reference and returns a result after finalization.

```text
                         poll_file_ready(cx)
                                   │ Ready(Ok(()))
                             enqueue_file(F)
                                   │
                                   ▼
                         ┌──────────────────┐
enqueue_frame(T) ─────────►│  WriteScheduler  │ ── next().await ──► FileResult
                         │     (Stream)     │
                         └──────────────────┘
      ▲
      │ Ready(Ok(()))
poll_frame_ready(cx)
```

Files and frames are injected independently using the Sink pattern. Each
successful readiness poll reserves one admission; the corresponding `enqueue_*`
call transfers ownership to the scheduler. On `Pending`, the producer keeps the
item and waits for a wakeup. Close each input with `close_files()` or
`close_frames()` when its producer finishes. `WriteScheduler` implements `Stream`;
`next().await` yields finalization results or errors in destination order, and
returns `None` after all results have drained.

```rust
use std::{
    collections::VecDeque,
    convert::Infallible,
    error::Error,
    future::{Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};
use futures::executor::block_on;
use carbon_io::{FrameBudget, FrameWriter, SchedulerConfig, WriteEvent, WriteFile, WriteScheduler};

struct File;
struct Writer(u32);

impl WriteFile<u32> for File {
    type Error = Infallible;
    type Output = u32;
    type Open = Ready<Result<Writer, Infallible>>;
    type Writer = Writer;

    fn frame_capacity(&self) -> u32 { 3 }
    fn open(&self) -> Self::Open { ready(Ok(Writer(0))) }
}

impl FrameWriter<u32> for Writer {
    type Error = Infallible;
    type Output = u32;

    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, frame: &u32)
        -> Poll<Result<(), Infallible>>
    {
        self.get_mut().0 += frame;
        Poll::Ready(Ok(()))
    }
    fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>)
        -> Poll<Result<u32, Infallible>>
    {
        Poll::Ready(Ok(self.0))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let budget = FrameBudget::new(64);
        let mut config = SchedulerConfig::default();
        let mut writer = WriteScheduler::new(&budget, &mut config);
        let mut files = VecDeque::from([File, File]);
        let mut frames = VecDeque::from([1, 2, 3, 4]);
        let mut results = Vec::new();

        while let Some(event) = writer.next_event().await? {
            match event {
                WriteEvent::FileReady => {
                    while !files.is_empty() && writer.accept_file() {
                        writer.enqueue_file(files.pop_front().unwrap())?;
                    }
                    if files.is_empty() {
                        writer.close_files()?;
                    }
                }
                WriteEvent::FrameReady => {
                    while !frames.is_empty() && writer.accept_frame() {
                        writer.enqueue_frame(frames.pop_front().unwrap())?;
                    }
                    if frames.is_empty() {
                        writer.close_frames()?;
                        writer.close_files()?;
                    }
                }
                WriteEvent::File(result) => results.push(result),
            }
        }
        assert_eq!(results, [6, 4]);
        Ok(())
    })
}
```

Input frames fill destinations in order, up to each destination's capacity.
Destinations open only after receiving a frame. The last file may be partial;
an empty input opens no file.

`accept_frame()` reserves budget capacity before `enqueue_frame()` takes the
frame. It returns `false` when the budget or current destination is unavailable.
Each successful enqueue consumes its reservation; the next call checks the
limits again. Calling `accept_file()` or `accept_frame()` repeatedly without an
enqueue preserves the same reservation. Neither method polls backend I/O.

Readiness is reported while its reservation remains available. If a producer
temporarily has no item, exclude that admission from the requested events:

```ignore
let interest = if frames.is_empty() {
    Interest::FILES | Interest::RESULTS
} else {
    Interest::ALL
};
let event = writer.next_event_with_interest(interest).await?;
```

`Interest::FILES` requests file admission, `FRAMES` requests write-frame
admission, and `RESULTS` requests ordered outputs. For reads, `RESULTS` yields
frames. Excluding an interest preserves its existing reservation and prevents
readiness from being returned repeatedly in a busy loop. Errors and EOF remain
visible with any mask. An exhausted producer closes its input; a temporarily
empty producer can be polled alongside this future in `select!`.

Enqueueing after a ready event does not wake the task again. If the last poll
returned `Pending`, the first enqueue or closure wakes the waiting task; further
mutations before its next poll share that notification. Backend and budget
wakers continue notifying the registered task independently.

When handling `WriteEvent::File(result)`, poll `progress()` while processing the
result to keep accepted files writing and finalizing:

```ignore
tokio::select! {
    outcome = process_result(result) => outcome?,
    _ = writer.progress() => unreachable!("progress never completes"),
}
```

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
are reported through the scheduler's output stream.

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

A write retry drops the failed attempt, reopens the file, and replays from the
first retained frame. Reservations may exceed `target_frames` to fit one file.
There is no global rollback if an earlier file fails.

A fatal error releases scheduler resources immediately. `next()` emits the
error once, then returns EOF. If `progress()` discovers the error, it saves it
for the next `next()`.

Dropping a pending `next()` or a `progress()` future preserves the scheduler's
state. Dropping the scheduler abandons its operations and releases its resources.
It does not undo writes. Backends must clean up abandoned attempts.

## Integrating a backend or custom stream

Both schedulers implement `Stream`.

| Entry point | Behavior |
| --- | --- |
| `next().await` / `poll_next(cx)` | Remove the next ordered output, advancing I/O if needed |
| `next_event().await` / `poll(cx)` | Return input readiness or the next ordered output with one mutable borrow |
| `next_event_with_interest(interest).await` / `poll_with_interest(cx, interest)` | Return only requested event kinds, errors or EOF |
| `file_ready().await` / `frame_ready().await` | Reserve one input admission without consuming output |
| `accept_file()` / `accept_frame()` | Reserve one admission synchronously, returning a bool without taking an item or polling backend I/O |
| `enqueue_file(file)` / `enqueue_frame(frame)` | Transfer ownership into a reserved admission |
| `progress()` | Keep advancing I/O without consuming output; never completes |
| `poll_progress(cx)` | Advance at most 256 work items from a custom future or stream |

Backends must follow these rules:

- Register the supplied waker before returning `Pending`.
- Make every `open()` an independent attempt that can be abandoned by dropping it.
- A pending `poll_write` receives the same logical frame on its next poll, but
  its address may change. Do not retain the borrowed `&T` after returning.
- Finalize only after all assigned writes succeed.

Frames need neither `Clone`, `Arc`, `Send`, nor `Unpin`. Input streams, opening
futures, readers, and writers may also be `!Unpin`. The crate contains no unsafe
code.

Invalid admissions return contract errors and release scheduler resources.
`FramePermit::poll_grow` returns `Poll<Result<(), ContractError>>`; an invalid
growth request returns `InvalidBudgetRequest` and preserves existing grants or
waits.

## Examples and performance

Each example is an independent crate with its own dependencies and lockfile.
See the [examples guide](examples/README.md) for the memory, TCP/file, Reqwest,
and Pingora uploads and their launch commands.

The [memory example](https://github.com/theking90000/carbon-io/blob/main/examples/memory/src/main.rs) puts both schedulers together using
`futures::select_biased!`:

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
