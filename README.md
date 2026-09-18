# io-scheduler

Ordered asynchronous I/O over opaque segmented files. Open and poll several
files concurrently, retain a bounded logical window, and emit frames or file
results in their original order.

The core depends only on `futures-core` and `pin-project-lite`. It creates no
threads, tasks, channels, or runtime. It does not require `Clone`, `Arc`, `Send`,
or `Unpin` on frames. Opening futures, readers, writers, and input streams may
also be `!Unpin`. The crate contains no `unsafe` code.

## Status and installation

Version 0.1.0. The repository is private and the crate is not published on
crates.io. With access to this repository:

```toml
[dependencies]
io-scheduler = { git = "ssh://git@github.com/theking90000/io-scheduler", branch = "main" }
futures = "0.3" # Only needed for the executor and StreamExt used below.
```

Rust 1.85 or newer, edition 2024. `futures` and the allocation instrumentation
library `stats_alloc` are development dependencies of this repository.

## Read

Implement `ReadFile<T>` for a complete logical file. Its reader must yield
exactly `frame_count()` successful frames and then EOF.

```rust
use std::{convert::Infallible, future::{Ready, ready}};
use futures::{executor::block_on, stream, StreamExt};
use io_scheduler::{FrameBudget, ReadFile, ReadScheduler, SchedulerConfig};

struct File(Vec<u32>);

impl ReadFile<u32> for File {
    type Error = Infallible;
    type Reader = stream::Iter<std::vec::IntoIter<Result<u32, Infallible>>>;
    type Open = Ready<Result<Self::Reader, Self::Error>>;

    fn frame_count(&self) -> u32 { self.0.len() as u32 }

    fn open(&self) -> Self::Open {
        // This in-memory adapter copies its data. The scheduler never clones T.
        ready(Ok(stream::iter(self.0.iter().copied().map(Ok).collect::<Vec<_>>())))
    }
}

block_on(async {
    let files = stream::iter([File(vec![0, 1, 2]), File(vec![3, 4])]);
    let mut reader = ReadScheduler::new(
        files, FrameBudget::new(64), SchedulerConfig::default(),
    );
    let mut values = Vec::new();
    while let Some(frame) = reader.next().await {
        values.push(frame.unwrap());
    }
    assert_eq!(values, [0, 1, 2, 3, 4]);
});
```

Opening and buffering are separate. With files of six and five frames, a window
of eight authorizes six frames from the first file and two from the second.
The second reader can finish those two while the first reader is pending.
Future files can be opened without being authorized to produce frames.

Read frames live in one flat ring per scheduler. Emission advances the logical
window. A final EOF probe detects extra frames; an early EOF or a reader error
terminates the stream. Only opening failures can be retried.

## Write

Implement `WriteFile<T>` and `FrameWriter<T>`. The writer receives `&T` and
returns a file result after finalization. Poll methods use `Pin<&mut Self>` so
writers may contain pinned asynchronous state.

```rust
use std::{convert::Infallible, future::{Ready, ready}, pin::Pin, task::{Context, Poll}};
use futures::{executor::block_on, stream, StreamExt};
use io_scheduler::{FrameBudget, FrameWriter, SchedulerConfig, WriteFile, WriteScheduler};

struct File;
struct Writer(u32);

impl WriteFile<u32> for File {
    type Error = Infallible;
    type Output = u32;
    type Writer = Writer;
    type Open = Ready<Result<Writer, Infallible>>;
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
        stream::iter([1, 2, 3, 4]), stream::iter([File, File]),
        FrameBudget::new(64), SchedulerConfig::default(),
    );
    assert_eq!(writer.next().await.unwrap().unwrap(), 6);
    assert_eq!(writer.next().await.unwrap().unwrap(), 4);
    assert!(writer.next().await.is_none());
});
```

Frames fill destinations strictly by capacity. Writers start on the first frame
and can progress while earlier writers are finalizing. Results remain ordered.
The final destination may be partial. An empty input finalizes no file, and
unused prefetched destinations are dropped.

| Retry setting | Frame lifetime | Capacity rule |
| --- | --- | --- |
| `max_retries == 0`, the default | Drop each frame immediately after a successful `poll_write` | A file may exceed the entire frame budget |
| `max_retries > 0` | Retain every frame until `poll_finalize` succeeds | Reserve a whole file before its first frame; file capacity must fit the global budget |

A retry drops the current attempt, reopens the complete file, and replays from
its first retained frame. `max_retries` counts additional attempts across
open/write/finalize failures. Successfully finalized future files release their
frames even while their results wait behind earlier files. There is no global
rollback if an earlier file later fails.

## Window and budget

`SchedulerConfig` contains `window: Window` and `max_retries: u32`.

| Window field | Default | Meaning |
| --- | --- | --- |
| `target_frames` | 256 | Desired local grant. Retry-enabled writes may exceed it to reserve a complete file |
| `open_ahead_frames` | 1024 | Discovery distance from the logical front; the effective horizon is at least `target_frames` |
| `max_active_files` | 16 | Maximum opening futures and live readers/writers |

A file is discovered if its beginning lies inside the horizon; its end can
extend beyond it. Frame counts and capacities must be nonzero. A zero target,
zero active-file limit, or zero global budget produces a contract error.

Clone a `FrameBudget` to share it. Permits reserve capacity in blocks, not once
per frame. Waiters use FIFO minimum-size requests. Capacity is reserved before
a waiter is woken, preventing competing schedulers from stealing its grant.
Reads request growth in blocks up to 32 frames. Small targets and small total
budgets remain supported. Strict FIFO can delay smaller requests behind a larger
one. Grants are not preempted.

`set_window(window)` updates either scheduler. A shrink preserves already
engaged frames and live files, then applies as they drain. It does not reallocate
the read ring or the reusable slot storage. Call `poll_next` again to drive a
window change. `granted_frames()` reports the local grant; writes also expose
`retained_frames()`.

The budget counts scheduler-owned frames and reservations, not bytes, backend
buffers, consumer-owned frames, or completed results. Size it for all components
that must progress together. In particular, a writer pulling from a reader that
shares its budget needs enough capacity for both grants. Holding all capacity
in the producer while the consumer waits for a reservation can deadlock; the
budget cannot infer application dependencies or revoke retained data.

## Polling and cancellation

Each input and I/O slot has a dedicated waker. External wakes are deduplicated
and routed to a local ready queue. Pending operations are polled again only
when notified; ready operations continue locally. There is no scan over dormant
slots, even with thousands of pending opens. Each poll processes at most 256
work items before yielding if necessary.

A scheduler advances only while its consumer polls it. Buffered read output is
drained in batches without shared-budget access. Without a background task, no
I/O progresses while the consumer stops polling entirely.

Pinned I/O storage and per-slot wakers allocate when the pool grows and are
reused across files. The read ring grows to the largest granted window. Write
buffers are reused per slot. This is not an allocation-free constructor or a
zero-byte-overhead abstraction; the normal frame path requires no allocation.

On `Pending`, backends must register the supplied waker. A writer must not retain
the borrowed `&T` after returning, and cannot rely on its address staying fixed
between polls. `open()` must create an independent attempt. Dropping an attempt
must release backend resources, including any unused prefetched destination.
Input streams yield files and frames directly; backend errors come from opening,
reading, writing, and finalizing those files.

Dropping a scheduler cancels its futures and drops its frames, readers, writers,
and permits. A fatal error releases them immediately, emits one
`SchedulerError`, then terminates. Both schedulers implement `FusedStream`.

## Development

```sh
cargo test --locked
cargo test --locked --release
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo run --locked --example memory
cargo bench --locked --bench scheduler
cargo package --locked
```

The tests cover the CDC invariants, pinned backends, cancellation, shared-budget
contention, no-retry retention, and wake routing over 10,000 pending files.
Benchmarks report throughput, backend polls, allocations per frame and file,
and allocated bytes. See [docs/benchmarks.md](docs/benchmarks.md) for method and
recorded measurements, and [docs/specification.md](docs/specification.md) for the
original French CDC and its overriding V1 amendments.

MIT licensed. Package publication is a separate manual step after the repository
is made public and the desired crates.io name is checked.
