use futures_core::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// An opaque, repeatably openable sequence of exactly `frame_count()` frames.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, future::{ready, Ready}, ops::Range};
/// use futures::stream;
/// use carbon_io::ReadFile;
///
/// struct MemFile(Range<u32>);
///
/// impl ReadFile<u32> for MemFile {
///     type Error = Infallible;
///     type Reader = stream::Iter<std::vec::IntoIter<Result<u32, Infallible>>>;
///     type Open = Ready<Result<Self::Reader, Self::Error>>;
///
///     fn frame_count(&self) -> u32 {
///         self.0.end - self.0.start
///     }
///     fn open(&self) -> Self::Open {
///         ready(Ok(stream::iter(self.0.clone().map(Ok).collect::<Vec<_>>())))
///     }
/// }
/// ```
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
/// Descriptors may be discovered ahead of time; defer resource creation to `open`.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, future::{ready, Ready}};
/// use carbon_io::{FrameWriter, WriteFile};
///
/// struct MemFile;
/// # struct MemWriter;
/// # impl FrameWriter<u32> for MemWriter {
/// #     type Error = Infallible;
/// #     type Output = u32;
/// #     fn poll_write(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, _: &u32) -> std::task::Poll<Result<(), Infallible>> { std::task::Poll::Ready(Ok(())) }
/// #     fn poll_finalize(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<u32, Infallible>> { std::task::Poll::Ready(Ok(0)) }
/// # }
///
/// impl WriteFile<u32> for MemFile {
///     type Error = Infallible;
///     type Output = u32;
///     type Open = Ready<Result<MemWriter, Infallible>>;
///     type Writer = MemWriter;
///
///     fn frame_capacity(&self) -> u32 { 10 }
///     fn open(&self) -> Self::Open { ready(Ok(MemWriter)) }
/// }
/// ```
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
    /// Called only after the scheduler has assigned at least one frame to this file.
    fn open(&self) -> Self::Open;
}

/// Borrowing writer which never owns the scheduler's frames.
///
/// On `Pending`, register the supplied waker. The next call receives the same
/// logical frame until success, but its address may change. Do not retain the
/// borrow after returning. Finalization may begin only after all writes succeed.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
/// use carbon_io::FrameWriter;
///
/// struct SumWriter(u32);
///
/// impl FrameWriter<u32> for SumWriter {
///     type Error = Infallible;
///     type Output = u32;
///
///     fn poll_write(
///         self: Pin<&mut Self>,
///         _: &mut Context<'_>,
///         frame: &u32,
///     ) -> Poll<Result<(), Infallible>> {
///         self.get_mut().0 += *frame;
///         Poll::Ready(Ok(()))
///     }
///
///     fn poll_finalize(
///         self: Pin<&mut Self>,
///         _: &mut Context<'_>,
///     ) -> Poll<Result<u32, Infallible>> {
///         Poll::Ready(Ok(self.0))
///     }
/// }
/// ```
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
