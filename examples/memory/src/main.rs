//! Keep buffering during consumer backpressure, without a background task.
use carbon_io::{
    AsyncFilesWrite, FrameAllocator, FrameBudget, PartialFrame, ReadEvent, ReadFile, ReadScheduler,
    SchedulerConfig, WriteFile, WriteScheduler,
};
use futures::{FutureExt, Stream, executor::block_on};
use std::{
    collections::VecDeque,
    convert::Infallible,
    future::{Ready, poll_fn, ready},
    pin::Pin,
    task::{Context, Poll},
};

struct Source(std::ops::Range<u32>);
struct Reader {
    values: std::ops::Range<u32>,
    waiting: bool,
}
impl Stream for Reader {
    type Item = Result<u32, Infallible>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Simulate an asynchronous read: Pending once before each result.
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.waiting = true;
        Poll::Ready(this.values.next().map(Ok))
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
        ready(Ok(Reader {
            values: self.0.clone(),
            waiting: true,
        }))
    }
}
struct Allocator;
struct Partial(Option<u32>);
impl FrameAllocator for Allocator {
    type Partial = Partial;
    fn allocate(&mut self) -> Partial {
        Partial(None)
    }
}
impl PartialFrame for Partial {
    type Input = u32;
    type Output = ();
    type Frame = u32;
    type Error = Infallible;
    fn poll_fill(&mut self, _: &Context<'_>, input: &u32) -> Poll<Result<(), Infallible>> {
        self.0 = Some(*input);
        Poll::Ready(Ok(()))
    }
    fn is_complete(&self) -> bool {
        self.0.is_some()
    }
    fn is_empty(&self) -> bool {
        self.0.is_none()
    }
    fn finish(self) -> u32 {
        self.0.unwrap()
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
    type Error = Infallible;
    type Output = u32;
    fn frame_capacity(&self) -> u32 {
        self.capacity
    }
    fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Infallible>> {
        self.get_mut().sum = 0;
        Poll::Ready(Ok(()))
    }
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
        frame: &u32,
    ) -> Poll<Result<(), Infallible>> {
        let this = self.get_mut();
        if this.waiting {
            this.waiting = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.waiting = true;
        this.sum += frame;
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<u32, Infallible>> {
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

fn main() {
    block_on(async {
        let budget = FrameBudget::new(512);
        let mut scheduler_config_1 = SchedulerConfig::default();
        let mut reader = ReadScheduler::new(&budget, &mut scheduler_config_1);
        let mut files = VecDeque::from([Source(0..3), Source(3..6)]);
        let mut frames = Vec::new();
        while let Some(event) = reader.next_event().await.unwrap() {
            match event {
                ReadEvent::FileReady => {
                    while !files.is_empty() && reader.accept_file() {
                        reader.enqueue_file(files.pop_front().unwrap()).unwrap();
                    }
                    if files.is_empty() {
                        reader.close_files().unwrap();
                    }
                }
                ReadEvent::Frame(frame) => {
                    // Awaiting consume() alone would suspend prefetching.
                    let value = futures::select_biased! {
                        _ = reader.progress().fuse() => unreachable!("progress never completes"),
                        value = consume(frame).fuse() => value,
                    };
                    frames.push(value);
                }
            }
        }
        assert_eq!(frames, [0, 1, 2, 3, 4, 5]);
        let mut config = SchedulerConfig::default().with_max_retries(2);
        config.window.max_active_files = 2;
        let mut allocator = Allocator;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        writer.push_file(Destination::new(2)).unwrap();
        writer.push_file(Destination::new(4)).unwrap();
        let mut sums = Vec::new();
        for frame in frames {
            loop {
                let status = poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, &frame))
                    .await
                    .unwrap();
                for _ in 0..status.completed_files {
                    let (_, sum) = writer.pop_swap(Destination::new(4)).unwrap();
                    sums.push(consume(sum).await);
                }
                if status.output.is_some() {
                    break;
                }
            }
        }
        loop {
            let completed = poll_fn(|cx| Pin::new(&mut writer).poll_close(cx))
                .await
                .unwrap();
            if completed == 0 {
                break;
            }
            for _ in 0..completed {
                let (_, sum) = writer.pop_file().unwrap().unwrap();
                sums.push(consume(sum).await);
            }
        }
        assert_eq!(sums, [1, 14]);
        println!("Finalized file sums: {sums:?}");
    });
}
