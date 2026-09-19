# Changelog

## Unreleased

- Rename the crate to `carbon-io`, with Rust imports using `carbon_io`.
- Rename the benchmark scale variable to `CARBON_IO_BENCH_SCALE`.
- Update documentation and repository links for CARBON I/O.

## 0.1.0

- Ordered read and write streams with independent opening and buffering windows.
- Flat read-frame ring and reusable pinned I/O slots with routed wakers.
- FIFO shared frame grants and whole-file reservations for replayable writes.
- Immediate frame release when retries are disabled.
- Runtime-independent cancellation, contract errors, tests and allocation benchmarks.
