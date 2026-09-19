# Benchmark method and recorded run

Run the standalone benchmark with:

```sh
cargo bench --locked --bench scheduler
IO_SCHEDULER_BENCH_SCALE=10 cargo bench --locked --bench scheduler
```

`benches/scheduler.rs` uses numeric frames and in-memory, statically dispatched
backends. Every case checks the resulting checksum. `stats_alloc` measures
allocations and reallocations, including scheduler construction, growth, and
backend setup inside the measured region. It is a development dependency only.
The final CSV columns count cumulative requested bytes, not peak resident memory.

The backpressure cases alternate `Pending` and `Ready` and wake the supplied
waker. They measure routing and polling overhead without network or disk delay.
The shared case interleaves four schedulers over one budget. The retry case
fails the first finalization of every file and verifies a complete replay.
`backend_polls_per_frame` excludes scheduler polls and input-stream polls.

The raw stream baseline applies no scheduling, buffering, or ordering across
files. Its difference from the scheduler cases makes the CPU overhead visible;
it is not an equivalent implementation of the contract.

## Recorded run

Recorded on 2026-09-18, macOS aarch64, rustc 1.93.0, optimized Cargo bench profile,
scale 1. Raw measurements are checked in as [benchmarks.csv](benchmarks.csv).
These are single-run smoke measurements, not statistical performance guarantees.
Allocator instrumentation and machine load affect timing. Repeat with a larger
scale before drawing conclusions about small differences.

| Case | Frames/s | Allocations per file |
| --- | ---: | ---: |
| Raw stream baseline | 469,300,695 | 0 |
| Read, one large file | 25,195,524 | 19 |
| Read, 10,000 four-frame files | 17,931,189 | 0.012 |
| Read, multiple active files | 30,398,159 | 0.450 |
| Read, synthetic backpressure | 13,766,520 | 2.120 |
| Read, many openings | 5,135,888 | 0.045 |
| Write, one large file, no retry | 32,697,102 | 21 |
| Write, 10,000 four-frame files | 19,434,769 | 0.029 |
| Write, multiple active files | 35,058,984 | 0.490 |
| Write, synthetic backpressure | 15,428,658 | 3.880 |
| Write, finalization retry | 27,167,208 | 1.130 |
| Four shared-budget readers | 38,298,886 | 17.750 |

The small-file allocation totals stay below one allocation per file because
pinned slots and wakers are reused. The single-file read costs approximately
39.7 ns/frame in this run, against 2.1 ns/frame for the unscheduled baseline.
Scheduling overhead therefore remains material for a trivial arithmetic source;
these measurements do not establish that it is negligible in every workload.

The wake-routing regression separately opens 10,000 gated files, wakes only slot
42, and verifies that none of the other 9,999 opening futures are polled again.
Its deterministic poll counts are more useful than a timing threshold for
catching an accidental scan of dormant futures.

## Explicit driving under consumer backpressure

Both schedulers now expose `poll_progress(cx)` and a never-completing `progress()`
future. The original benchmark cases still consume with `next()` only. Four new
cases run on `futures::executor::block_on` and add four self-waking consumer waits
per emitted frame or file result. The `*_consumer_drive` variant polls `progress()`
through `select_biased!` while waiting; `*_consumer_wait` only polls the consumer.
The driver is first in the select to make the polling order reproducible.
Neither branch is heap-boxed. The executor is initialized before measurement.

These waits and the backend's `Pending` responses have no real latency. The new
cases measure CPU overhead of cooperative polling and selection, not the possible
throughput benefit of overlapping network/disk waits. A real I/O benchmark is
needed to measure that benefit. Do not compare the new cases directly with the
older `read_backpressure` and `write_backpressure` cases, which use a different
consumer and polling loop.

Recorded on 2026-09-18 at scale 100. The table uses medians of five runs of the
final implementation. The before column is one run captured before editing.
[Raw measurements](benchmarks-drive.csv) retain every run. These sequential local
runs are indicative and are not a statistically controlled regression estimate.

| Existing case | Before, Mframes/s | After median, Mframes/s | Change |
| --- | ---: | ---: | ---: |
| `raw_stream_baseline` | 470.40 | 472.78 | +0.5% |
| `read_one_large_file` | 43.30 | 42.19 | -2.6% |
| `read_many_small_files` | 27.73 | 27.94 | +0.8% |
| `read_multiple_active` | 40.40 | 39.27 | -2.8% |
| `read_backpressure` | 16.64 | 16.96 | +1.9% |
| `read_many_openings` | 6.13 | 6.23 | +1.6% |
| `write_one_large_file` | 37.57 | 35.23 | -6.2% |
| `write_many_small_files` | 19.98 | 19.25 | -3.7% |
| `write_multiple_active` | 36.56 | 35.65 | -2.5% |
| `write_backpressure` | 16.14 | 16.00 | -0.9% |
| `write_finalize_retry` | 26.44 | 26.07 | -1.4% |
| `shared_four_schedulers` | 40.83 | 42.18 | +3.3% |

All original cases retained identical backend poll counts and allocation metrics.
Timing differences range from about -6% to +3%; this run does not establish zero
throughput regression. The largest observed drop is the single-file write case.

| Consumer case | Median ns/frame | Allocated bytes, entire run |
| --- | ---: | ---: |
| `read_consumer_wait` | 102.2 | 2152 |
| `read_consumer_drive` | 212.7 | 2152 |
| `write_consumer_wait` | 90.2 | 12272 |
| `write_consumer_drive` | 130.6 | 12272 |

The consumer pairs have identical backend poll counts and allocation totals.
Explicit driving adds about 111 ns per read frame and 40 ns per written frame in
this synthetic workload. Writes emit one result per four frames, so their added
cost is about 162 ns per consumed result. This includes select bookkeeping and
extra driver polls. No synchronization primitive was added, but each driver poll
still uses the existing ready queue's wake-registration locks.

The deterministic tests in `tests/drive.rs` verify that the driver advances I/O
without consuming output, stops at the read window or completed-write horizon,
routes backend and budget wakeups, yields after bounded work, preserves pending
operations on cancellation, and releases resources on a fatal error while saving
that error for a single subsequent stream emission.
