//! Frames are movable values even if their type does not implement Unpin or Clone.
use futures::{StreamExt, executor::block_on, stream};
use io_scheduler::{FrameBudget, FrameWriter, SchedulerConfig, WriteFile, WriteScheduler};
use std::{
    convert::Infallible,
    future::{Ready, ready},
    marker::PhantomPinned,
    pin::Pin,
    task::{Context, Poll},
};
struct Frame(u32, PhantomPinned);
struct File;
struct Writer(u32);
impl WriteFile<Frame> for File {
    type Error = Infallible;
    type Output = u32;
    type Open = Ready<Result<Writer, Infallible>>;
    type Writer = Writer;
    fn frame_capacity(&self) -> u32 {
        4
    }
    fn open(&self) -> Self::Open {
        ready(Ok(Writer(0)))
    }
}
impl FrameWriter<Frame> for Writer {
    type Error = Infallible;
    type Output = u32;
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &Frame,
    ) -> Poll<Result<(), Infallible>> {
        self.get_mut().0 += frame.0;
        Poll::Ready(Ok(()))
    }
    fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Infallible>> {
        Poll::Ready(Ok(self.0))
    }
}
#[test]
fn neither_clone_nor_unpin_is_required_on_frames() {
    for max_retries in [0, 1] {
        let input = stream::iter([Frame(2, PhantomPinned), Frame(3, PhantomPinned)]);
        let mut s = WriteScheduler::new(
            input,
            stream::iter([File]),
            FrameBudget::new(4),
            SchedulerConfig {
                max_retries,
                ..SchedulerConfig::default()
            },
        );
        block_on(async {
            assert_eq!(s.next().await.unwrap().unwrap(), 5);
            assert!(s.next().await.is_none());
        });
    }
}
