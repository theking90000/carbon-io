//! Keep buffering during consumer backpressure, without a background task.
use carbon_io::{
    FrameBudget, FrameWriter, ReadEvent, ReadFile, ReadScheduler, SchedulerConfig, WriteEvent,
    WriteFile, WriteScheduler,
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
struct Destination(u32);
struct Writer {
    sum: u32,
    waiting: bool,
}
impl WriteFile<u32> for Destination {
    type Error = Infallible;
    type Output = u32;
    type Open = Ready<Result<Writer, Infallible>>;
    type Writer = Writer;
    fn frame_capacity(&self) -> u32 {
        self.0
    }
    fn open(&self) -> Self::Open {
        ready(Ok(Writer {
            sum: 0,
            waiting: true,
        }))
    }
}
impl FrameWriter<u32> for Writer {
    type Error = Infallible;
    type Output = u32;
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
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
    fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Infallible>> {
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
        let mut frames = VecDeque::from(frames);
        let mut files = VecDeque::from([Destination(2), Destination(4)]);
        let mut scheduler_config_2 = SchedulerConfig::default().with_max_retries(2);
        let mut writer = WriteScheduler::new(&budget, &mut scheduler_config_2);
        let mut sums = Vec::new();
        while let Some(event) = writer.next_event().await.unwrap() {
            match event {
                WriteEvent::FileReady => {
                    while !files.is_empty() && writer.accept_file() {
                        writer.enqueue_file(files.pop_front().unwrap()).unwrap();
                    }
                    if files.is_empty() {
                        writer.close_files().unwrap();
                    }
                }
                WriteEvent::FrameReady => {
                    while !frames.is_empty() && writer.accept_frame() {
                        writer.enqueue_frame(frames.pop_front().unwrap()).unwrap();
                    }
                    if frames.is_empty() {
                        writer.close_frames().unwrap();
                        writer.close_files().unwrap();
                    }
                }
                WriteEvent::File(result) => {
                    // Keep other files writing while the previous result is consumed.
                    let sum = futures::select_biased! {
                        _ = writer.progress().fuse() => unreachable!("progress never completes"),
                        sum = consume(result).fuse() => sum,
                    };
                    sums.push(sum);
                }
            }
        }
        assert_eq!(sums, [1, 14]);
        println!("Finalized file sums: {sums:?}");
    });
}
