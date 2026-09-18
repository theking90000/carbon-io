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
