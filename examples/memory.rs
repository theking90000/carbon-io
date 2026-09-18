//! In-memory read and write adapters using no particular async runtime.
use futures::{Stream, StreamExt, executor::block_on, stream};
use io_scheduler::{
    FrameBudget, FrameWriter, ReadFile, ReadScheduler, SchedulerConfig, WriteFile, WriteScheduler,
};
use std::{
    convert::Infallible,
    future::{Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};

struct Source(std::ops::Range<u32>);
struct Reader(std::ops::Range<u32>);
impl Stream for Reader {
    type Item = Result<u32, Infallible>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.next().map(Ok))
    }
}
impl ReadFile<u32> for Source {
    type Error = Infallible;
    type Open = Ready<Result<Reader, Infallible>>;
    type Reader = Reader;
    fn frame_count(&self) -> u32 {
        self.0.end - self.0.start
    }
    fn open(&self) -> Self::Open {
        ready(Ok(Reader(self.0.clone())))
    }
}
struct Destination(u32);
struct Writer(u32);
impl WriteFile<u32> for Destination {
    type Error = Infallible;
    type Output = u32;
    type Open = Ready<Result<Writer, Infallible>>;
    type Writer = Writer;
    fn frame_capacity(&self) -> u32 {
        self.0
    }
    fn open(&self) -> Self::Open {
        ready(Ok(Writer(0)))
    }
}
impl FrameWriter<u32> for Writer {
    type Error = Infallible;
    type Output = u32;
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        frame: &u32,
    ) -> Poll<Result<(), Infallible>> {
        self.get_mut().0 += frame;
        Poll::Ready(Ok(()))
    }
    fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Infallible>> {
        Poll::Ready(Ok(self.0))
    }
}
fn main() {
    block_on(async {
        let budget = FrameBudget::new(512);
        let mut reader = ReadScheduler::new(
            stream::iter([Source(0..3), Source(3..6)]),
            budget.clone(),
            SchedulerConfig::default(),
        );
        let mut frames = Vec::new();
        while let Some(frame) = reader.next().await {
            frames.push(frame.unwrap());
        }
        assert_eq!(frames, [0, 1, 2, 3, 4, 5]);
        let mut writer = WriteScheduler::new(
            stream::iter(frames),
            stream::iter([Destination(4), Destination(4)]),
            budget,
            SchedulerConfig {
                max_retries: 2,
                ..SchedulerConfig::default()
            },
        );
        let mut sums = Vec::new();
        while let Some(result) = writer.next().await {
            sums.push(result.unwrap());
        }
        assert_eq!(sums, [6, 9]);
        println!("Finalized file sums: {sums:?}");
    });
}
