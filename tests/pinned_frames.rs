//! Frames and partial frames can be movable without implementing Unpin or Clone.
use carbon_io::{
    AsyncFilesWrite, FrameAllocator, FrameBudget, PartialFrame, SchedulerConfig, WriteFile,
    WriteScheduler,
};
use std::{
    convert::Infallible,
    marker::PhantomPinned,
    pin::Pin,
    task::{Context, Poll},
};
struct Frame(u32, PhantomPinned);
struct Partial(Option<Frame>, PhantomPinned);
struct Allocator;
impl FrameAllocator for Allocator {
    type Partial = Partial;
    fn allocate(&mut self) -> Partial {
        Partial(None, PhantomPinned)
    }
}
impl PartialFrame for Partial {
    type Input = Frame;
    type Output = ();
    type Frame = Frame;
    type Error = Infallible;
    fn poll_fill(&mut self, _: &Context<'_>, input: &Frame) -> Poll<Result<(), Infallible>> {
        self.0 = Some(Frame(input.0, PhantomPinned));
        Poll::Ready(Ok(()))
    }
    fn is_complete(&self) -> bool {
        self.0.is_some()
    }
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    fn finish(self) -> Frame {
        self.0.unwrap()
    }
}
struct File(u32);
impl WriteFile<Frame> for File {
    type Error = Infallible;
    type Output = u32;
    fn frame_capacity(&self) -> u32 {
        4
    }
    fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Infallible>> {
        self.get_mut().0 = 0;
        Poll::Ready(Ok(()))
    }
    fn poll_write(
        self: Pin<&mut Self>,
        _: &Context<'_>,
        frame: &Frame,
    ) -> Poll<Result<(), Infallible>> {
        self.get_mut().0 += frame.0;
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<u32, Infallible>> {
        Poll::Ready(Ok(self.0))
    }
}
#[test]
fn neither_clone_nor_unpin_is_required_on_frames_partial_frames_or_input() {
    for max_retries in [0, 1] {
        let budget = FrameBudget::new(4);
        let mut cfg = SchedulerConfig::default().with_max_retries(max_retries);
        let mut allocator = Allocator;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        writer.push_file(File(0)).unwrap();
        let waker = futures::task::noop_waker();
        let cx = Context::from_waker(&waker);
        for input in [Frame(2, PhantomPinned), Frame(3, PhantomPinned)] {
            let Poll::Ready(Ok(status)) = Pin::new(&mut writer).poll_write(&cx, &input) else {
                panic!("not consumed")
            };
            assert_eq!(status.output, Some(()));
        }
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(1)));
        assert_eq!(writer.pop_file().unwrap().unwrap().1, 5);
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(0)));
        assert_eq!(budget.available_capacity(), 4);
    }
}
