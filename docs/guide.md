# Scheduling and backend guide

See the [README](../README.md) for runnable examples.

## Complete frames and sequential files

A frame is an opaque value `T`. Each `ReadFile<T>` opens and reads on the same
object. Its stable, nonzero `frame_count()` announces exactly how many complete
frames precede EOF. There is one sequential read operation per file, while
operations on different files can be in flight concurrently.

`ReadFile::poll_read(cx, &mut frame)` returns `Pending`, a complete frame with
`Some(())`, EOF with `None`, or a backend error. The backend keeps its own offsets
and preparation state. It never keeps a reference to the borrowed buffer.
After `Pending`, CARBON presents the same logical buffer and preserves its
contents; its address may change. On a new frame, the backend receives a recycled
buffer and overwrites or resets it. CARBON has no partial-frame read abstraction.

Direct files must be `Unpin`. `Pin<Box<F>>` and `Pin<&mut F>` forward `ReadFile`
for pinned backends. Frames need neither `Clone` nor `Unpin`.

## Admission and explicit polling

Construct `ReadScheduler` with a buffer factory, shared `&FrameBudget`, and
mutable `&mut SchedulerConfig`. A factory passed as `&mut factory` remains
caller-owned. It is invoked only after budget authorization and only when no
recycled buffer is available.

`push_file(file)` returns immediately. On refusal it returns `(PushError, file)`;
`Full` means retry later, `Closed` means admission has ended, and `Contract`
reports an invalid descriptor. Refused admissions do not terminate accepted
work or consume deferred backend errors. No readiness reservation exists.

`close_files()` is idempotent. Accepted files continue until every declared frame
has been consumed. While admission remains open,
an empty scheduler returns `Pending`, registers no waker, and requests no budget.

Mutations do not wake the caller. After `push_file`, `close_files`, or `set_window`,
explicitly call `poll_read` or `poll_progress` again. An external descriptor
producer supplies its own wakeups. A combined producer/consumer retries a held
file after a read has freed admission, then explicitly repolls after a
successful push. Waiting only on that empty read would not wake the producer.

## Ordered buffering and anticipatory opening

The window has three controls:

| Field | Default | Meaning |
| --- | ---: | --- |
| `target_frames` | 256 | Desired contiguous buffering window |
| `open_ahead_frames` | 1024 | Descriptor admission and anticipatory opening horizon |
| `max_active_files` | 16 | Maximum admitted files, including opening and draining files |

The admission horizon is at least `target_frames`. A descriptor can enter if
its first frame lies inside that horizon, even if the file extends beyond it.
Admitting starts no I/O. Subsequent polling can open the file before its frames
receive buffering capacity.

Budget authorization follows global frame order. For files A and B of 100 frames
each, `open_ahead_frames = 120` permits opening B immediately. With
`target_frames = 10`, B cannot read until 91 frames of A have been consumed.
There is no distribution of capacity for fairness between files. Files inside
the authorized window can complete frames out of order; output remains ordered.

The shared `FrameBudget` counts reservations, pending buffers, and complete buffered
frames. It does not count bytes, backend
storage, idle recycled allocations, or frames already handed to the consumer.
Its FIFO, batched grants and synchronization are unchanged. When reading and
writing with one shared budget, leave capacity for both schedulers to progress.

Shrinking a window preserves authorized operations and buffers. Allocation
capacity of recycled buffers is retained. `granted_frames()` reads the local
grant without synchronizing with the shared budget.

## Output, EOF, and errors

`ReadScheduler::poll_read(cx, &mut output)` advances I/O and swaps the next
complete frame with the caller's reusable buffer. Heap-backed payload bytes
are not copied, and the frame requires no `Clone` implementation.
`Pending`, EOF, and errors leave the output unchanged. The delivered frame
belongs to the caller and is outside the shared budget. An `AsyncRead` adapter
would need to copy into its caller's byte slice and account for its current
frame until fully consumed; no such adapter is provided by this API.

CARBON trusts the declared `frame_count()` and stops polling the file after that
many complete frames. An earlier `None` is `UnexpectedEof`; extra frames are
never polled. After delivering the declared last frame, CARBON removes the file
from the active window and recycles the returned buffer. The next file can
advance without an EOF probe or a dedicated EOF buffer.

Opening errors retry up to `max_retries`. The next `poll_open` must reset the
failed opening attempt. Reading errors and premature EOF are terminal.
Terminal failure drops I/O and buffers and returns the budget immediately.
`poll_read` reports the error once and subsequently returns EOF. If
`poll_progress` discovers the error, it retains it for the next `poll_read`.

Dropping a waiting future leaves the scheduler intact. Dropping the scheduler
abandons its operations and returns its budget. Backends own cancellation and
cleanup of their operations.

## Progress and wakeups

`poll_read` and `poll_progress` use one engine. `poll_progress` consumes no output;
`progress()` wraps it in a future that never completes, including at EOF or on
error. Poll it alongside the consumer's wait with `select!` to prepare more data
while the consumer is busy. Without either kind of polling, nothing advances.

Files and budget waits use the caller's context directly. There are no per-file
wakers, wake queues, or additional mutexes. Pending operations are revisited on
the next scheduler call, at most once per operation in each call. A rotating
cursor gives each admitted file an opportunity to advance. Work is limited to
`max(256, active_files)` backend polls, allowing one complete pass even with a
configured window larger than 256. If runnable work remains at the limit,
CARBON requests another poll. A pending-only pass does not self-wake.

## Writing

`WriteScheduler` borrows its frame allocator, shared budget, and configuration.
`PartialFrame` assembles borrowed input, and `WriteFile` opens, writes and closes
on the same object. Fill the destination FIFO with `push_file`, replace completed
heads with `pop_swap`, then end input with `poll_close` and drain results through
`pop_file`. Destinations open only after their first frame is assigned.

Without retries, accepted frames can be released individually. With retries,
a whole destination must fit the shared budget and its frames stay available
for replay until close succeeds. Closing flushes a nonempty partial frame and
drops unused destinations without opening them.

The [memory example](../examples/memory/src/main.rs) shows frame polling and
progress during consumer waits. The [benchmark report](benchmarks.md) contains
historical measurements; they do not measure this new read implementation.
The [specification](specification.md) retains the original design history.
