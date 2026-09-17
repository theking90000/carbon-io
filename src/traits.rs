use futures_core::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// An opaque, repeatably openable sequence of exactly `frame_count()` frames.
pub trait ReadFile<T> {
    /// Backend failure.
    type Error;
    /// Asynchronous open operation. May be `!Unpin`.
    type Open: Future<Output = Result<Self::Reader, Self::Error>>;
    /// Ordered frames followed by EOF. May be `!Unpin`.
    type Reader: Stream<Item = Result<T, Self::Error>>;
    /// Exact, nonzero number of frames.
    fn frame_count(&self) -> u32;
    /// Open the complete logical file, without offsets or ranges.
    fn open(&self) -> Self::Open;
}

/// An opaque destination. Reopening must start an independent complete attempt.
pub trait WriteFile<T> {
    /// Backend failure.
    type Error;
    /// Successful finalization result.
    type Output;
    /// Asynchronous open operation. May be `!Unpin`.
    type Open: Future<Output = Result<Self::Writer, Self::Error>>;
    /// Writer for one attempt. May be `!Unpin`.
    type Writer: FrameWriter<T, Error = Self::Error, Output = Self::Output>;
    /// Nonzero maximum number of frames in this destination.
    fn frame_capacity(&self) -> u32;
    /// Open a new attempt. Dropping an old attempt must cancel its work.
    fn open(&self) -> Self::Open;
}

/// Borrowing writer which never owns the scheduler's frames.
///
/// On `Pending`, register the supplied waker. The next call receives the same
/// logical frame until success, but its address may change. Do not retain the
/// borrow after returning. Finalization may begin only after all writes succeed.
pub trait FrameWriter<T> {
    /// Backend failure.
    type Error;
    /// Successful finalization result.
    type Output;
    /// Accept one frame, or arrange a wakeup when progress becomes possible.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>>;
    /// Finish the file. Success makes previously accepted frames disposable.
    /// With retries disabled, acceptance by `poll_write` already releases them.
    fn poll_finalize(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>>;
}
