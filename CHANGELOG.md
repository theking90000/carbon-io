# Changelog

## 0.3.0

- Comply with Rust API Guidelines (RFC 430 checklist).
- Implement `Debug` for all public types (`FrameBudget`, `FramePermit`, `ReadScheduler`, `WriteScheduler`).
- Derive `Hash` for `Window`, `SchedulerConfig`, `ContractError`, and `SchedulerError`.
- Derive `Copy` for `SchedulerError<E>` when `E: Copy`.
- Mark `Window`, `SchedulerConfig`, and `ContractError` with `#[non_exhaustive]` for future-proof API evolution.
- Add `Window::new` constructor and fluent builder methods (`with_target_frames`, `with_open_ahead_frames`, `with_max_active_files`, `with_window`, `with_max_retries`).
- Add comprehensive rustdoc examples for all public types and primary methods.
- Document error conditions with `# Errors` sections.
- Exclude documentation and CI workflow directories from crate package.
- Add crate metadata (`authors`, `documentation`, `homepage`).

## 0.2.0

- Open write destinations only after their first frame is assigned; drop unused descriptors unopened.
- Add `poll_progress` to advance schedulers without consuming output.
- Rename the crate to `carbon-io`, with Rust imports using `carbon_io`.
- Rename the benchmark scale variable to `CARBON_IO_BENCH_SCALE`.
- Add `tcp_to_file` streaming example.
- Update documentation and repository links for CARBON I/O.

## 0.1.0

- Ordered read and write streams with independent opening and buffering windows.
- Flat read-frame ring and reusable pinned I/O slots with routed wakers.
- FIFO shared frame grants and whole-file reservations for replayable writes.
- Immediate frame release when retries are disabled.
- Runtime-independent cancellation, contract errors, tests and allocation benchmarks.
