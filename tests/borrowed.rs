//! Caller-owned streams, cancellation, and input mutation without shared state.
mod support;

use carbon_io::{ContractError, FrameBudget, ReadScheduler, SchedulerError, WriteScheduler};
use futures::{Stream, StreamExt, executor::block_on, stream};
use pin_project_lite::pin_project;
use std::{
    cell::Cell,
    collections::VecDeque,
    marker::PhantomPinned,
    pin::{Pin, pin},
    rc::Rc,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
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

struct Input {
    frames: VecDeque<Frame>,
    closed: bool,
    polls: Rc<Cell<usize>>,
    waker: Option<Waker>,
}
impl Input {
    fn new() -> Self {
        Self {
            frames: VecDeque::new(),
            closed: false,
            polls: Rc::new(Cell::new(0)),
            waker: None,
        }
    }
    // The scheduler's input_mut() supplies the wakeup for synchronous changes.
    fn push(&mut self, frame: Frame) {
        self.frames.push_back(frame);
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
impl Stream for Input {
    type Item = Frame;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Frame>> {
        let this = self.get_mut();
        this.polls.set(this.polls.get() + 1);
        if let Some(frame) = this.frames.pop_front() {
            Poll::Ready(Some(frame))
        } else if this.closed {
            Poll::Ready(None)
        } else {
            this.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[test]
fn input_mut_feeds_an_unpin_input_and_wakes_the_scheduler() {
    let mut input = Input::new();
    let input_polls = input.polls.clone();
    let mut files = pin!(stream::iter([Write::new(4)]));
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 4, 1, 0);
    let mut scheduler = WriteScheduler::new(&mut input, &mut files, &budget, &mut cfg);
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut scheduler).poll_next(&mut cx).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    let drops = Rc::new(Cell::new(0));
    scheduler.input_mut().push(Frame {
        value: 7,
        drops: drops.clone(),
    });
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    scheduler.poll_progress(&mut cx);
    assert_eq!(drops.get(), 1);
    // Progress in writers must not continuously repoll the pending input.
    let before = input_polls.get();
    scheduler.poll_progress(&mut cx);
    scheduler.poll_progress(&mut cx);
    assert_eq!(input_polls.get(), before);
    scheduler.input_mut().push(Frame {
        value: 9,
        drops: drops.clone(),
    });
    scheduler.input_mut().close();
    assert_eq!(collect(&mut scheduler), [Ok(vec![7, 9])]);
    assert_eq!(drops.get(), 2);
    let polls = input_polls.get();
    scheduler.input_mut();
    for _ in 0..3 {
        assert_eq!(poll(&mut scheduler), Poll::Ready(None));
    }
    assert_eq!(input_polls.get(), polls);
    let new_window = config(2, 4, 1, 0).window;
    scheduler.set_window(new_window).unwrap();
    drop(scheduler);
    assert_eq!(cfg.window, new_window);
    assert!(input.closed);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn dropping_a_writer_preserves_unread_frames_and_destinations() {
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
    let mut scheduler = WriteScheduler::new(&mut input, &mut files, &budget, &mut cfg);
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
fn write_eof_keeps_both_pinned_streams_alive_and_is_final() {
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
        let mut scheduler = WriteScheduler::new(&mut input, &mut files, &budget, &mut cfg);
        assert_eq!(collect(&mut scheduler), [Ok(vec![0, 1])]);
        assert_eq!(input_drops.get(), 0);
        assert_eq!(files_drops.get(), 0);
        let polls = input_polls.get();
        scheduler.input_mut();
        assert_eq!(poll(&mut scheduler), Poll::Ready(None));
        assert_eq!(input_polls.get(), polls);
        drop(scheduler);
        assert_eq!(block_on(files.as_mut().next()).unwrap().capacity, 4);
    }
    assert_eq!(input_drops.get(), 1);
    assert_eq!(files_drops.get(), 1);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn write_failure_preserves_the_input_and_remaining_files() {
    let (input, drops) = frames(3);
    let mut input = pin!(PinnedStream::new(input));
    let mut files = pin!(stream::iter([Write::new(0), Write::new(4)]));
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 1, 1, 0);
    let mut scheduler = WriteScheduler::new(&mut input, &mut files, &budget, &mut cfg);
    assert_eq!(
        poll(&mut scheduler),
        Poll::Ready(Some(Err(SchedulerError::Contract(
            ContractError::ZeroFrameCapacity
        ),)))
    );
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
    let mut scheduler = ReadScheduler::new(&mut files, &budget, &mut cfg);
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
        let mut scheduler = ReadScheduler::new(&mut files, &budget, &mut cfg);
        assert_eq!(collect(&mut scheduler), [Ok(3)]);
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
    let mut scheduler = ReadScheduler::new(&mut files, &budget, &mut cfg);
    assert_eq!(
        poll(&mut scheduler),
        Poll::Ready(Some(Err(SchedulerError::Backend("read"))))
    );
    drop(scheduler);
    assert_eq!(budget.available_capacity(), 1);
    assert_eq!(block_on(files.as_mut().next()).unwrap().values, [Ok(2)]);
}
