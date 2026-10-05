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

/// A destination that opens, accepts frames, and closes on the same object.
///
/// Resource creation starts only when `poll_open` is first called. Each `Pending`
/// poll registers the supplied waker and continues the same operation on the next
/// poll. A pending write receives the same logical frame, whose address may change;
/// the frame reference must never be retained after returning.
///
/// After any operation returns an error, the next `poll_open` must cancel the old
/// attempt and open a fresh destination for replay from the first frame. Repeated
/// pending open polls continue that new attempt. Dropping the file cancels its I/O.
/// Closing succeeds only after all accepted frames have been committed.
///
/// Files stored directly in the scheduler must be `Unpin`. A `!Unpin` file can
/// instead be supplied as `Pin<Box<F>>` or `Pin<&mut F>`; both forward this trait.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
/// use carbon_io::WriteFile;
///
/// struct SumFile(u32);
/// impl WriteFile<u32> for SumFile {
///     type Error = Infallible;
///     type Output = u32;
///
///     fn frame_capacity(&self) -> u32 { 10 }
///     fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Infallible>> {
///         self.get_mut().0 = 0;
///         Poll::Ready(Ok(()))
///     }
///     fn poll_write(self: Pin<&mut Self>, _: &Context<'_>, frame: &u32)
///         -> Poll<Result<(), Infallible>>
///     {
///         self.get_mut().0 += *frame;
///         Poll::Ready(Ok(()))
///     }
///     fn poll_close(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<u32, Infallible>> {
///         Poll::Ready(Ok(self.get_mut().0))
///     }
/// }
/// ```
pub trait WriteFile<T> {
    /// Backend failure.
    type Error;
    /// Successful close result.
    type Output;
    /// Nonzero maximum number of frames in this destination.
    fn frame_capacity(&self) -> u32;
    /// Open the first attempt, or restart after an error, without replacing this file.
    fn poll_open(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<(), Self::Error>>;
    /// Accept one borrowed frame, or register a wakeup when writing can continue.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>>;
    /// Commit accepted frames and finish this file.
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>>;
}

impl<T, P> WriteFile<T> for Pin<P>
where
    P: std::ops::DerefMut + Unpin,
    P::Target: WriteFile<T>,
{
    type Error = <P::Target as WriteFile<T>>::Error;
    type Output = <P::Target as WriteFile<T>>::Output;

    fn frame_capacity(&self) -> u32 {
        self.as_ref().get_ref().frame_capacity()
    }

    fn poll_open(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().as_mut().poll_open(cx)
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>> {
        self.get_mut().as_mut().poll_write(cx, frame)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        self.get_mut().as_mut().poll_close(cx)
    }
}
