//! Caller-owned sources, explicit write admissions, and cancellation.
mod support;

use carbon_io::{FrameBudget, ReadScheduler, SchedulerError};
use futures::{Stream, StreamExt, executor::block_on, stream};
use pin_project_lite::pin_project;
use std::{
    cell::Cell,
    marker::PhantomPinned,
    pin::{Pin, pin},
    rc::Rc,
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

#[test]
fn write_borrows_allocator_and_config_and_leaves_external_sources_to_caller() {
    use carbon_io::{AsyncFilesWrite, WriteScheduler};
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 2, 1, 0);
    let mut allocator = Allocator::default();
    let drops = allocator.drops.clone();
    let mut values = [7, 9, 11].into_iter();
    let first = Write::new(4);
    first.writing.close();
    let second = Write::new(4);
    let mut files = [first.clone(), second.clone()].into_iter();
    let window = config(1, 2, 1, 0).window;
    {
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        writer.push_file(files.next().unwrap()).unwrap();
        for _ in 0..2 {
            write_value(&mut writer, values.next().unwrap());
        }
        writer.set_window(window).unwrap();
    }
    assert_eq!(values.next(), Some(11));
    assert_eq!(files.next().unwrap().capacity, 4);
    assert_eq!(second.opens.get(), 0);
    assert_eq!(drops.get(), 2);
    assert_eq!(cfg.window, window);
    assert_eq!(budget.available_capacity(), 2);
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(second).unwrap();
    write_value(&mut writer, 11);
    assert_eq!(close_writer(&mut writer), [vec![11]]);
}
