use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// A sequential source that opens and reads on the same object.
///
/// `frame_count()` is exact, nonzero and stable. Creation starts only on the
/// first `poll_open`. After an opening error, the next `poll_open` starts a fresh
/// attempt; repeated pending polls continue that attempt.
///
/// `poll_read` returns `Some(())` only for a complete frame, and `None` for EOF.
/// CARBON stops reading after `frame_count()` complete frames, without polling
/// for EOF. An earlier `None` is a contract error; extra frames are never polled.
/// The backend owns its offsets and preparation state. After `Pending`, it
/// receives the same logical buffer with its contents preserved, but its address
/// may change. No buffer reference may be retained between polls. A new read may
/// receive a recycled buffer: overwrite or reset its previous contents as needed.
/// Every `Pending` operation must register the supplied waker. Dropping the file
/// abandons its operations. Reading errors are terminal in [`crate::ReadScheduler`].
///
/// Directly stored files must be `Unpin`; `Pin<Box<F>>` and `Pin<&mut F>` forward
/// this trait for pinned files. Frames need neither `Clone` nor `Unpin`.
pub trait ReadFile<T> {
    /// Backend failure.
    type Error;
    /// Exact, nonzero number of complete frames.
    fn frame_count(&self) -> u32;
    /// Open the first attempt, or start a new attempt after an opening error.
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
    /// Fill one complete frame, continue its preparation, or report EOF.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &mut T,
    ) -> Poll<Result<Option<()>, Self::Error>>;
}

impl<T, P> ReadFile<T> for Pin<P>
where
    P: std::ops::DerefMut + Unpin,
    P::Target: ReadFile<T>,
{
    type Error = <P::Target as ReadFile<T>>::Error;

    fn frame_count(&self) -> u32 {
        self.as_ref().get_ref().frame_count()
    }

    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().as_mut().poll_open(cx)
    }

    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &mut T,
    ) -> Poll<Result<Option<()>, Self::Error>> {
        self.get_mut().as_mut().poll_read(cx, frame)
    }
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
///     fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
///         self.get_mut().0 = 0;
///         Poll::Ready(Ok(()))
///     }
///     fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, frame: &u32)
///         -> Poll<Result<(), Infallible>>
///     {
///         self.get_mut().0 += *frame;
///         Poll::Ready(Ok(()))
///     }
///     fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Infallible>> {
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
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
    /// Accept one borrowed frame, or register a wakeup when writing can continue.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>>;
    /// Commit accepted frames and finish this file.
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
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

    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().as_mut().poll_open(cx)
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &T,
    ) -> Poll<Result<(), Self::Error>> {
        self.get_mut().as_mut().poll_write(cx, frame)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        self.get_mut().as_mut().poll_close(cx)
    }
}
