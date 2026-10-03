//! Caller-owned sources, explicit write admissions, and cancellation.
mod support;

use carbon_io::{ContractError, FrameBudget, ReadScheduler, SchedulerError, WriteScheduler};
use futures::{Stream, StreamExt, executor::block_on, stream};
use pin_project_lite::pin_project;
use std::{
    cell::Cell,
    marker::PhantomPinned,
    pin::{Pin, pin},
    rc::Rc,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

pin_project! {
    struct Tracked<S> {
        #[pin]
        inner: S,
        drops: Rc<Cell<usize>>,
        polls: Rc<Cell<usize>>,
        #[pin]
        _pin: PhantomPinned,
    }
    impl<S> PinnedDrop for Tracked<S> {
        fn drop(this: Pin<&mut Self>) {
            this.drops.set(this.drops.get() + 1);
        }
    }
}
impl<S> Tracked<S> {
    fn new(inner: S, drops: Rc<Cell<usize>>, polls: Rc<Cell<usize>>) -> Self {
        Self {
            inner,
            drops,
            polls,
            _pin: PhantomPinned,
        }
    }
}
impl<S: Stream> Stream for Tracked<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.polls.set(this.polls.get() + 1);
        this.inner.poll_next(cx)
    }
}

#[test]
fn push_admissions_wake_the_scheduler_and_update_the_callers_config() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 4, 1, 0);
    let mut scheduler = WriteScheduler::new(&budget, &mut cfg);
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut scheduler).poll_next(&mut cx).is_pending());
    assert!(matches!(
        Pin::new(&mut scheduler).poll_file_ready(&mut cx),
        Poll::Ready(Ok(()))
    ));
    let before = wakes.0.load(Ordering::Relaxed);
    Pin::new(&mut scheduler)
        .enqueue_file(Write::new(4))
        .unwrap();
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    let drops = Rc::new(Cell::new(0));
    for value in [7, 9] {
        assert!(matches!(
            Pin::new(&mut scheduler).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        let before = wakes.0.load(Ordering::Relaxed);
        Pin::new(&mut scheduler)
            .enqueue_frame(Frame {
                value,
                drops: drops.clone(),
            })
            .unwrap();
        assert!(wakes.0.load(Ordering::Relaxed) > before);
    }
    Pin::new(&mut scheduler).close_frames().unwrap();
    Pin::new(&mut scheduler).close_files().unwrap();
    assert_eq!(collect(&mut scheduler), [Ok(vec![7, 9])]);
    assert_eq!(drops.get(), 2);
    for _ in 0..3 {
        assert_eq!(poll(&mut scheduler), Poll::Ready(None));
    }
    let new_window = config(2, 4, 1, 0).window;
    scheduler.set_window(new_window).unwrap();
    drop(scheduler);
    assert_eq!(cfg.window, new_window);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn dropping_a_writer_preserves_unread_external_frames_and_destinations() {
    let (input, drops) = frames(8);
    let mut input = pin!(input);
    let first = Write::new(4);
    first.writing.close();
    let second = Write::new(4);
    let mut files = pin!(PinnedStream::new(stream::iter([
        first.clone(),
        second.clone()
    ])));
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 1, 1, 0);
    let mut scheduler = WriteScheduler::new(&budget, &mut cfg);
    admit_file(&mut scheduler, block_on(files.as_mut().next()).unwrap());
    for _ in 0..2 {
        admit_frame(&mut scheduler, block_on(input.as_mut().next()).unwrap());
    }
    assert!(poll(&mut scheduler).is_pending());
    assert_eq!(scheduler.retained_frames(), 2);
    drop(scheduler);
    assert_eq!(drops.get(), 2);
    assert_eq!(first.drops.get(), 1);
    assert_eq!(budget.available_capacity(), 2);
    assert_eq!(block_on(input.as_mut().next()).unwrap().value, 2);
    assert_eq!(block_on(files.as_mut().next()).unwrap().capacity, 4);
    assert_eq!(second.opens.get(), 0);
}

#[test]
fn write_eof_does_not_poll_or_drop_external_pinned_streams() {
    let input_drops = Rc::new(Cell::new(0));
    let files_drops = Rc::new(Cell::new(0));
    let input_polls = Rc::new(Cell::new(0));
    let files_polls = Rc::new(Cell::new(0));
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 1, 1, 0);
    {
        let (input, _) = frames(2);
        let mut input = pin!(Tracked::new(
            input,
            input_drops.clone(),
            input_polls.clone()
        ));
        let mut files = pin!(Tracked::new(
            stream::iter([Write::new(4), Write::new(4)]),
            files_drops.clone(),
            files_polls.clone(),
        ));
        let mut scheduler = WriteScheduler::new(&budget, &mut cfg);
        admit_file(&mut scheduler, block_on(files.as_mut().next()).unwrap());
        for _ in 0..2 {
            admit_frame(&mut scheduler, block_on(input.as_mut().next()).unwrap());
        }
        Pin::new(&mut scheduler).close_frames().unwrap();
        Pin::new(&mut scheduler).close_files().unwrap();
        let before = (input_polls.get(), files_polls.get());
        assert_eq!(collect(&mut scheduler), [Ok(vec![0, 1])]);
        assert_eq!(input_drops.get(), 0);
        assert_eq!(files_drops.get(), 0);
        assert_eq!(poll(&mut scheduler), Poll::Ready(None));
        assert_eq!((input_polls.get(), files_polls.get()), before);
        drop(scheduler);
        assert_eq!(block_on(files.as_mut().next()).unwrap().capacity, 4);
    }
    assert_eq!(input_drops.get(), 1);
    assert_eq!(files_drops.get(), 1);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn write_failure_preserves_external_input_and_remaining_files() {
    let (input, drops) = frames(3);
    let mut input = pin!(PinnedStream::new(input));
    let mut files = pin!(stream::iter([Write::new(0), Write::new(4)]));
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 1, 1, 0);
    let mut scheduler = WriteScheduler::new(&budget, &mut cfg);
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut scheduler).poll_file_ready(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(
        Pin::new(&mut scheduler).enqueue_file(block_on(files.as_mut().next()).unwrap()),
        Err(SchedulerError::Contract(ContractError::ZeroFrameCapacity))
    );
    assert_eq!(poll(&mut scheduler), Poll::Ready(None));
    assert_eq!(drops.get(), 0);
    assert_eq!(budget.available_capacity(), 4);
    drop(scheduler);
    assert_eq!(block_on(input.as_mut().next()).unwrap().value, 0);
    assert_eq!(block_on(files.as_mut().next()).unwrap().capacity, 4);
}

#[test]
fn read_cancellation_preserves_remaining_files_and_updates_the_callers_config() {
    let first = Read::new([1]);
    first.reading.close();
    let mut files = pin!(PinnedStream::new(stream::iter([
        first.clone(),
        Read::new([2])
    ])));
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let new_window = config(1, 2, 1, 0).window;
    let mut scheduler = ReadScheduler::new(&budget, &mut cfg);
    admit_read_file(&mut scheduler, block_on(files.as_mut().next()).unwrap());
    assert!(poll(&mut scheduler).is_pending());
    scheduler.set_window(new_window).unwrap();
    drop(scheduler);
    assert_eq!(cfg.window, new_window);
    assert_eq!(first.drops.get(), 1);
    assert_eq!(budget.available_capacity(), 1);
    assert_eq!(block_on(files.as_mut().next()).unwrap().values, [Ok(2)]);
}

#[test]
fn read_eof_does_not_drop_or_repoll_the_callers_stream() {
    let drops = Rc::new(Cell::new(0));
    let polls = Rc::new(Cell::new(0));
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    {
        let mut files = pin!(Tracked::new(
            stream::iter([Read::new([3])]),
            drops.clone(),
            polls.clone(),
        ));
        let mut scheduler = ReadScheduler::new(&budget, &mut cfg);
        admit_read_file(&mut scheduler, block_on(files.as_mut().next()).unwrap());
        Pin::new(&mut scheduler).close_files().unwrap();
        let before = polls.get();
        assert_eq!(collect(&mut scheduler), [Ok(3)]);
        assert_eq!(polls.get(), before);
        assert_eq!(drops.get(), 0);
        let count = polls.get();
        for _ in 0..3 {
            assert_eq!(poll(&mut scheduler), Poll::Ready(None));
        }
        assert_eq!(polls.get(), count);
        assert_eq!(budget.available_capacity(), 1);
    }
    assert_eq!(drops.get(), 1);
}

#[test]
fn read_failure_preserves_remaining_descriptors() {
    let mut first = Read::new([1]);
    first.values = vec![Err("read")];
    let mut files = pin!(PinnedStream::new(stream::iter([first, Read::new([2])])));
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let mut scheduler = ReadScheduler::new(&budget, &mut cfg);
    admit_read_file(&mut scheduler, block_on(files.as_mut().next()).unwrap());
    Pin::new(&mut scheduler).close_files().unwrap();
    assert_eq!(
        poll(&mut scheduler),
        Poll::Ready(Some(Err(SchedulerError::Backend("read"))))
    );
    drop(scheduler);
    assert_eq!(budget.available_capacity(), 1);
    assert_eq!(block_on(files.as_mut().next()).unwrap().values, [Ok(2)]);
}
