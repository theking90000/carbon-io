//! Keep buffering during consumer backpressure, without a background task.
use carbon_io::{
    AsyncFilesWrite, FrameAllocator, FrameBudget, PartialFrame, ReadFile, ReadScheduler,
    SchedulerConfig, WriteFile, WriteScheduler,
};
use futures::{FutureExt, executor::block_on};
use std::{
    convert::Infallible,
    error::Error,
    io,
    future::poll_fn,
    pin::Pin,
    task::{Context, Poll},
};

struct Source {
    values: std::ops::Range<u32>,
    count: u32,
    waiting: bool,
}
impl Source {
    fn new(values: std::ops::Range<u32>) -> Self {
        Self { count: values.end.saturating_sub(values.start), values, waiting: true }
    }
}
impl ReadFile<u32> for Source {
    type Error = Infallible;
    fn frame_count(&self) -> u32 { self.count }
    fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, frame: &mut u32) -> Poll<Result<Option<()>, Infallible>> {
        let this = self.get_mut();
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.waiting = true;
        match this.values.next() {
            Some(value) => { *frame = value; Poll::Ready(Ok(Some(()))) },
            None => Poll::Ready(Ok(None)),
        }
    }
}
struct Allocator;
struct Partial { value: u32, complete: bool }
impl FrameAllocator for Allocator {
    type Partial = Partial;
    fn allocate(&mut self) -> Partial {
        Partial { value: 0, complete: false }
    }
}
impl PartialFrame for Partial {
    type Input = u32;
    type Output = ();
    type Frame = u32;
    type Error = Infallible;
    fn poll_fill(&mut self, _: &Context<'_>, input: &u32) -> Poll<Result<(), Infallible>> {
        self.value = *input;
        self.complete = true;
        Poll::Ready(Ok(()))
    }
    fn is_complete(&self) -> bool {
        self.complete
    }
    fn is_empty(&self) -> bool {
        !self.complete
    }
    fn finish(self) -> u32 {
        self.value
    }
}
struct Destination {
    capacity: u32,
    sum: u32,
    waiting: bool,
}
impl Destination {
    fn new(capacity: u32) -> Self {
        Self {
            capacity,
            sum: 0,
            waiting: true,
        }
    }
}
impl WriteFile<u32> for Destination {
    type Error = io::Error;
    type Output = u32;
    fn frame_capacity(&self) -> u32 {
        self.capacity
    }
    fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sum = 0;
        Poll::Ready(Ok(()))
    }
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &u32,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.waiting = true;
        this.sum = this.sum.checked_add(*frame).ok_or_else(|| io::Error::other("sum overflow"))?;
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Self::Error>> {
        Poll::Ready(Ok(self.sum))
    }
}
// A real consumer might await a socket or another asynchronous operation.
// Yield once here so the example runs without a network, timer, or runtime.
async fn consume<T>(value: T) -> T {
    let mut waiting = true;
    poll_fn(|cx| {
        if std::mem::replace(&mut waiting, false) {
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    value
}

fn main() -> Result<(), Box<dyn Error>> {
    block_on(async {
        let budget = FrameBudget::new(512);
        let mut scheduler_config_1 = SchedulerConfig::default();
        let mut reader = ReadScheduler::new(|| 0, &budget, &mut scheduler_config_1)?;
        reader.push_file(Source::new(0..3)).map_err(|(error, _)| error)?;
        reader.push_file(Source::new(3..6)).map_err(|(error, _)| error)?;
        reader.close_files();
        let mut frame = 0;
        let mut frames = Vec::new();
        while poll_fn(|cx| reader.poll_read(cx, &mut frame)).await?.is_some() {
            let value = futures::select_biased! {
                _ = reader.progress().fuse() => return Err(io::Error::other("progress unexpectedly completed").into()),
                value = consume(frame).fuse() => value,
            };
            frames.push(value);
        }
        assert_eq!(frames, [0, 1, 2, 3, 4, 5]);
        let mut config = SchedulerConfig::default().with_max_retries(2);
        config.window.max_active_files = 2;
        let mut allocator = Allocator;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config)?;
        writer.push_file(Destination::new(2))?;
        writer.push_file(Destination::new(4))?;
        let mut sums = Vec::new();
        for frame in frames {
            loop {
                let status = poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, &frame))
                    .await?;
                for _ in 0..status.completed_files {
                    let (_, sum) = writer.pop_swap(Destination::new(4))?;
                    sums.push(consume(sum).await);
                }
                if status.output.is_some() {
                    break;
                }
            }
        }
        loop {
            let completed = poll_fn(|cx| Pin::new(&mut writer).poll_close(cx))
                .await?;
            if completed == 0 {
                break;
            }
            for _ in 0..completed {
                let (_, sum) = writer.pop_file()?.ok_or_else(|| io::Error::other("missing close result"))?;
                sums.push(consume(sum).await);
            }
        }
        assert_eq!(sums, [1, 14]);
        println!("Finalized file sums: {sums:?}");
        Ok(())
    })
}
