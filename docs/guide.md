# Scheduling and backend guide

See the [README](../README.md) for installation and runnable examples.

## Frames and backends

CARBON treats each file as an ordered sequence of frames. A frame is one value
of your chosen Rust type, such as a chunk of bytes or a record. You provide the
async backend that opens, reads, and writes files. Those files may live on disk,
on a remote service, or in memory.

CARBON stands for Concurrent Async Reordering Buffered Ordered Nonblocking I/O:

- Multiple files can have operations in flight at once.
- Async I/O runs within your application's execution flow, without background
  tasks, threads, or channels.
- Operations may complete out of order. CARBON retains early results until
  their predecessors can be emitted.
- Bounded buffering limits how far work can advance ahead of the consumer.
- Consumers receive outputs in their logical order.
- Pending I/O does not block the calling task or other ready operations.

The crate uses Rust edition 2024 and depends on `futures-core` and
`pin-project-lite`. It works with any compatible async runtime. The README's
in-memory backends return ready results; a backend may also return pending I/O.

## Reading files

`ReadScheduler` takes a stream of file descriptors. Each descriptor declares
its frame count and opens a stream that yields exactly that many successful
frames, followed by EOF.

Files may open and produce frames concurrently. A later file can fill its part
of the buffer while an earlier file waits for I/O. CARBON emits frames in file
order and preserves the order of frames within each file.

```text
Input files
   |
   v
ReadScheduler<T>
   File 1: I/O pending
   File 2: read complete, frames buffered
   File 3: opened ahead
   |
   v
Output Stream<T>: file 1, then file 2, then file 3
```

Opening ahead and buffering ahead have separate limits. A file can be opened
before there is capacity to read its frames.

## Writing files

`WriteScheduler` takes a stream of frames and a stream of destinations. It fills
destinations in order, up to their declared frame capacities, and emits one
finalization result per file in destination order.

CARBON calls a destination's `open()` only after assigning its first frame.
Writing can start without waiting for the file to fill. Other writers can
advance while an earlier file finalizes.

```text
Input frames
   |
   v
WriteScheduler<T>
   File 1: finalizing
   File 2: writing
   File 3: unopened, waiting for its first frame
   |
   v
Output results: file 1, then file 2, then file 3
```

The last file may be partially filled. An empty input opens no file. You can
generate destination descriptors lazily without knowing the total input length.
Keep descriptors lightweight and defer resource creation to `open()`. CARBON
drops unused descriptors unopened.

Completed results remain limited by the discovery horizon until consumed. The
scheduler does not keep opening destinations indefinitely while the consumer
is blocked.

## Progress while the consumer waits

Consuming a scheduler's stream drives I/O. If the consumer awaits other work
between calls to `next()`, it stops polling the scheduler. Spare buffer capacity
does not start work by itself. A backend wakeup wakes the task, which still
needs to poll the scheduler.

Poll `progress()` alongside the consumer's async work to keep I/O advancing:

```ignore
while let Some(frame) = reader.next().await {
    tokio::select! {
        result = write_tcp(frame?) => result?,
        _ = reader.progress() => unreachable!("progress never completes"),
    }
}
```

Without `progress()`, the scheduler pauses during the send. With it, later files
can advance and upcoming frames can be buffered while TCP applies backpressure.
Once the window is full and no other work is runnable, the scheduler waits for
a wakeup.

`progress()` consumes no output and never completes, so it cannot win the
`select!` and cancel the send. Dropping it when the send finishes preserves
buffered frames and pending I/O. The same pattern keeps subsequent files
writing and finalizing while the consumer processes a write result.

Put the `select!` in the task that awaits the consumer. A custom stream that is
no longer polled cannot keep buffering. Synchronous blocking work also blocks
progress in the same task.

## Windows and shared budgets

`SchedulerConfig` holds a `Window` and a retry count. The window controls how
far the scheduler may work ahead of the consumer:

| Window field | Default | Controls |
| --- | ---: | --- |
| `target_frames` | 256 | Desired local frame capacity |
| `open_ahead_frames` | 1024 | How far ahead, in frames, files may be discovered |
| `max_active_files` | 16 | Maximum active files, including unopened write destinations |

The discovery horizon is at least `target_frames`. A file can be discovered if
its beginning lies inside that horizon, even if its end extends beyond it.
For reads, a window of eight frames over files of six and five frames authorizes
all six frames of the first file and the first two of the second.

Discovery may open readers ahead of time. For writes, it only prepares
descriptors. `open_ahead_frames` never triggers opening a destination without
an assigned frame.

Clone a `FrameBudget` to share capacity across schedulers. It counts
scheduler-owned frames and reservations, not bytes, backend buffers, frames
already handed to the consumer, or completed write results.

Grants are reserved in blocks. Waiting requests are served in FIFO order and
are not preempted. A large request can delay smaller requests behind it.

When a writer pulls from a reader sharing the same budget, leave enough capacity
for both to progress. If the reader holds all capacity while the writer waits
for a reservation, the pipeline can deadlock. The budget cannot infer these
dependencies or reclaim retained data.

Use `set_window(window)` to change either scheduler's window. Shrinking preserves
already authorized frames and opened files, then takes effect as work drains.
Keep polling to advance under the new limits. `granted_frames()` reports the
local capacity grant; writers also expose `retained_frames()`. Reusable buffers
do not shrink their allocated storage when the window shrinks.

Frame counts, destination capacities, the global budget, `target_frames`, and
`max_active_files` must be nonzero. The scheduler reports invalid configurations
and file contracts through its output stream.

## Retries, errors, and cancellation

`max_retries` defaults to zero and counts additional attempts per file. Reads
retry opening failures only. A reader error, early EOF, or extra frame is fatal.
Writes can retry opening, writing, or finalizing, with one shared retry count
across those stages.

Without write retries, each successful `poll_write` releases its frame, so a
file may exceed the global frame budget. With retries enabled, the scheduler
reserves capacity for the whole file before accepting its first frame and
retains every frame until successful finalization. The file must fit the global
budget, though its reservation may exceed `target_frames`.

A write retry drops the failed attempt, reopens the file, and replays from the
first retained frame. Successfully finalized files release their frames even
if their results are waiting behind an earlier file. There is no global
rollback if that earlier file subsequently fails.

A fatal error releases scheduler resources immediately. `next()` emits the
error once, then returns EOF. If `progress()` discovers the error, it saves it
for the next `next()` and stays pending. It also stays pending at EOF. Awaiting
`progress()` alone will never finish.

Both schedulers implement `FusedStream`. `is_terminated()` remains false while
an error is waiting to be emitted.

Dropping a pending `next()` or `progress()` future preserves the scheduler's
state. Dropping the scheduler itself abandons its operations, drops its frames
and backends, and returns its budget permits. This does not undo writes already
performed. Backends must clean up abandoned attempts, including writes
interrupted before finalization.

## Backend and custom stream integration

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
then call `progress()` while processing it. `next()` and `progress()` cannot run
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

## Implementation and performance

The schedulers route wakeups to individual slots rather than scanning dormant
operations. Read frames occupy one ring buffer. Write buffers, pinned I/O slots,
and per-slot wakers are reused. Storage allocates as it grows. Buffered read
output retains its fast path, and advancing I/O uses the existing ready queue
and shared-budget synchronization.

Polling `progress()` adds CPU work even though it adds no buffer or task. The
[benchmark report](benchmarks.md) records throughput, backend polls, allocations,
and the overhead of using `select!` during simulated backpressure. These
in-memory measurements do not establish a throughput gain for real network or
disk I/O.

The [memory example](../examples/memory/src/main.rs) puts both schedulers together using
`futures::select_biased!`. It simulates async reads, writes, and consumer waits
without a network or timer.

Tests cover ordering, capacity limits, retries, pinned backends, cancellation,
progress under backpressure, and wake routing over 10,000 pending files. See
the [original specification](specification.md) for the French CDC and its V1
amendments.
