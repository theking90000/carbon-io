# io-scheduler

Read and write several files concurrently, buffer a bounded amount of work, and
receive the results in their original order.

A file is a logical sequence of frames. You provide the asynchronous backend and
choose what a frame contains; the scheduler handles opening files ahead of time,
distributing capacity, and polling the operations that can make progress.
`ReadScheduler` emits frames. `WriteScheduler` emits one result per finalized
file.

The library runs inside your task. Use `next()` to consume output and
`progress()` to keep I/O moving while you await a consumer. Both use the same
scheduler state and buffers. No background task, thread, or channel is created.

## Installation

With access to the Git repository:

```toml
[dependencies]
io-scheduler = { git = "ssh://git@github.com/theking90000/io-scheduler", branch = "main" }
futures = "0.3" # StreamExt and the executor used in the examples.
```

Requires Rust 1.85 or newer, edition 2024. The library depends only on
`futures-core` and `pin-project-lite` and works with any compatible async runtime.
The Tokio snippets below assume your application already uses Tokio.

## Reading files

Implement `ReadFile<T>` for your file descriptors. Each descriptor declares its
frame count and opens a stream that yields exactly that many successful frames,
followed by EOF. Pass a stream of descriptors to `ReadScheduler`:

```rust
use std::{convert::Infallible, future::{Ready, ready}, ops::Range};
use futures::{executor::block_on, stream, StreamExt};
use io_scheduler::{FrameBudget, ReadFile, ReadScheduler, SchedulerConfig};

struct File(Range<u32>);

impl ReadFile<u32> for File {
    type Error = Infallible;
    type Reader = stream::Iter<std::vec::IntoIter<Result<u32, Infallible>>>;
    type Open = Ready<Result<Self::Reader, Self::Error>>;

    fn frame_count(&self) -> u32 { self.0.end - self.0.start }

    fn open(&self) -> Self::Open {
        // A small in-memory backend. Real backends can return pending I/O.
        ready(Ok(stream::iter(self.0.clone().map(Ok).collect::<Vec<_>>())))
    }
}

block_on(async {
    let mut reader = ReadScheduler::new(
        stream::iter([File(0..3), File(3..5)]),
        FrameBudget::new(64),
        SchedulerConfig::default(),
    );

    let mut values = Vec::new();
    while let Some(frame) = reader.next().await {
        values.push(frame.unwrap());
    }
    assert_eq!(values, [0, 1, 2, 3, 4]);
});
```

Files may open and produce frames concurrently, but the consumer always receives
frames in file order. A later file can fill its part of the buffer while an
earlier file waits for I/O. Opening ahead and buffering ahead have separate
limits, so a file can be opened before there is capacity to read its frames.

### Keep reading while the consumer waits

The loop above immediately asks for the next frame. A network consumer often
awaits a send between calls to `next()`. During that wait, it no longer polls the
scheduler, so the scheduler stops filling its buffer. Having spare capacity does
not start work by itself. A backend wakeup wakes the task; the task still needs
to poll the scheduler.

Use `select!` to poll `progress()` alongside the send:

```ignore
while let Some(frame) = reader.next().await {
    tokio::select! {
        result = write_tcp(frame?) => result?,
        _ = reader.progress() => unreachable!("progress never completes"),
    }
}
```

Now, while TCP applies backpressure, the scheduler can keep filling its existing
window. Once the window is full and no other work is runnable, it waits for a
wakeup. `progress()` consumes no frames and never completes, so it cannot win the
`select!` and cancel the send. When the send finishes, dropping `progress()`
leaves the buffered frames and pending I/O available for the next iteration.

Put this `select!` in the task that awaits the consumer. A custom stream that
is itself no longer polled cannot keep buffering. Synchronous blocking work also
blocks progress in the same task.

## Writing files

Implement `WriteFile<T>` to describe a destination and `FrameWriter<T>` to write
its frames. A destination declares its frame capacity. Its writer accepts frames
by reference and returns a result after finalization:

```rust
use std::{convert::Infallible, future::{Ready, ready}, pin::Pin, task::{Context, Poll}};
use futures::{executor::block_on, stream, StreamExt};
use io_scheduler::{FrameBudget, FrameWriter, SchedulerConfig, WriteFile, WriteScheduler};

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

block_on(async {
    let mut writer = WriteScheduler::new(
        stream::iter([1, 2, 3, 4]),
        stream::iter([File, File]),
        FrameBudget::new(64),
        SchedulerConfig::default(),
    );

    assert_eq!(writer.next().await.unwrap().unwrap(), 6);
    assert_eq!(writer.next().await.unwrap().unwrap(), 4);
    assert!(writer.next().await.is_none());
});
```

Input frames fill destinations in order, up to each destination's capacity.
Writing starts as soon as the first frame arrives. Other writers can advance
while an earlier file finalizes; results are still emitted in destination order.
The last file may be partial. An empty input finalizes no file, and unused opened
destinations are dropped.

If processing a result involves an asynchronous wait, use the same pattern as
for reads to keep subsequent files writing and finalizing:

```ignore
while let Some(result) = writer.next().await {
    tokio::select! {
        outcome = process_result(result?) => outcome?,
        _ = writer.progress() => unreachable!("progress never completes"),
    }
}
```

Completed results remain limited by the discovery horizon until consumed. The
scheduler does not keep opening destinations indefinitely while the consumer
is blocked.

## Choosing the window and budget

`SchedulerConfig` holds a `Window` and a retry count. The window determines how
far the scheduler may work ahead of the consumer:

| Window field | Default | Controls |
| --- | ---: | --- |
| `target_frames` | 256 | Desired local frame capacity |
| `open_ahead_frames` | 1024 | How far ahead, in frames, files may be discovered |
| `max_active_files` | 16 | Maximum simultaneous opens and live readers or writers |

The discovery horizon is at least `target_frames`. A file can be discovered if
its beginning lies inside that horizon, even if its end extends beyond it.
For reads, a window of eight frames over files of six and five frames authorizes
all six frames of the first file and the first two of the second.

`FrameBudget` limits capacity across schedulers. Clone it to share one budget.
It counts scheduler-owned frames and reservations, not bytes, backend buffers,
frames already handed to the consumer, or completed write results. Grants are
reserved in blocks; waiting requests are served in FIFO order and are not
preempted. A large request can therefore delay smaller requests behind it.

When a writer pulls from a reader sharing the same budget, leave enough capacity
for both to progress. If the reader holds all capacity while the writer waits
for a reservation, the pipeline can deadlock. The budget cannot infer those
dependencies or reclaim retained data.

Use `set_window(window)` to change either scheduler's window. Shrinking preserves
already authorized frames and opened files, then takes effect as work drains.
Keep polling to advance under the new limits. `granted_frames()` reports the
local capacity grant; writers also expose `retained_frames()`. The reusable
buffers do not shrink their allocated storage when the window shrinks.

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
Successfully finalized files release their frames even if their results are
waiting behind an earlier file. There is no global rollback if that earlier
file subsequently fails.

A fatal error releases scheduler resources immediately. `next()` emits the
error once, then returns EOF. If `progress()` discovers the error, it saves it
for the next `next()` and stays pending. It also stays pending at EOF; awaiting
`progress()` alone will never finish. Both schedulers implement `FusedStream`,
and `is_terminated()` remains false while an error is waiting to be emitted.

Dropping a pending `next()` or a `progress()` future preserves the scheduler's
state. Dropping the scheduler itself abandons its operations, drops its frames
and backends, and returns its budget permits. This does not undo writes already
performed. Backends are responsible for cleaning up abandoned attempts,
including destinations opened ahead of time but never used.

## Integrating a backend or custom stream

Both schedulers implement `Stream`. Their input streams yield file descriptors
and, for writes, frames directly. Backend errors come from opening, reading,
writing, or finalizing.

| Entry point | Behavior |
| --- | --- |
| `next().await` / `poll_next(cx)` | Remove the next ordered output, advancing I/O if needed |
| `progress()` | Keep advancing I/O without consuming output; never completes |
| `poll_progress(cx)` | Advance at most 256 work items from a custom future or stream |

`poll_progress(cx)` registers the caller for wakeups and requests another poll
if runnable work remains after its work limit. Otherwise it waits for a backend
or budget notification. The caller must arrange to poll it again. `progress()`
wraps that operation in a future for use with `select!`.

Each operation exclusively borrows the scheduler. Retrieve an output first,
then call `progress()` while processing it; `next()` and `progress()` cannot run
concurrently on the same instance.

Backends must follow these rules:

- Register the supplied waker before returning `Pending`.
- Make every `open()` an independent attempt that can be abandoned by dropping it.
- A pending `poll_write` receives the same logical frame on its next poll, but
  its address may change. Do not retain the borrowed `&T` after returning.
- Finalize only after all assigned writes succeed.

Frames need neither `Clone`, `Arc`, `Send`, nor `Unpin`. Input streams, opening
futures, readers, and writers may also be `!Unpin`. The crate contains no unsafe
code.

## Examples and performance

The [memory example](examples/memory.rs) puts both schedulers together using
`futures::select_biased!`. It simulates asynchronous reads, writes, and consumer
waits without a network or timer:

```sh
cargo run --locked --example memory
```

The schedulers route wakeups to individual slots rather than scanning dormant
operations. Read frames occupy one ring buffer; write buffers, pinned I/O slots,
and per-slot wakers are reused. Storage allocates as it grows. Buffered read
output retains its fast path, and advancing I/O uses the existing ready queue
and shared-budget synchronization.

Polling `progress()` adds CPU work even though it adds no buffer or task. The
[benchmark report](docs/benchmarks.md) records throughput, backend polls,
allocations, and the overhead of using `select!` during simulated backpressure.
Those in-memory measurements do not establish a throughput gain for real
network or disk I/O.

## Development

```sh
cargo test --locked
cargo test --locked --release
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo bench --locked --bench scheduler
```

Tests cover ordering, capacity limits, retries, pinned backends, cancellation,
progress under backpressure, and wake routing over 10,000 pending files. The
[original specification](docs/specification.md) contains the French CDC and its
V1 amendments.

MIT licensed.
