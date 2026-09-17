//! Waker routing, bounded work, backpressure, and non-starvation tests.
mod support;
use futures::{Stream, StreamExt, stream};
use io_scheduler::{FrameBudget, ReadScheduler, WriteScheduler};
use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn no_busy_loop_when_all_sources_pending() {
    let a = Read::new(0..2);
    a.opening.close();
    let b = Read::new(2..4);
    b.opening.close();
    let mut s = ReadScheduler::new(
        stream::iter([a.clone(), b.clone()]),
        FrameBudget::new(4),
        config(4, 100, 10, 0),
    );
    let (counter, w) = context_waker();
    let mut cx = Context::from_waker(&w);
    assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
    let before = counter.0.load(Ordering::Relaxed);
    for _ in 0..10 {
        assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
    }
    assert_eq!(counter.0.load(Ordering::Relaxed), before);
    assert_eq!(a.opening.polls(), 1);
    assert_eq!(b.opening.polls(), 1);
}
#[test]
fn only_woken_slot_is_polled_among_ten_thousand_pending_opens() {
    let files: Vec<_> = (0..10_000)
        .map(|i| {
            let f = Read::new([i]);
            f.opening.close();
            f
        })
        .collect();
    let mut s = ReadScheduler::new(
        stream::iter(files.clone()),
        FrameBudget::new(32),
        config(32, 20_000, 10_000, 0),
    );
    for _ in 0..200 {
        assert!(poll(&mut s).is_pending());
    }
    assert!(files.iter().all(|f| f.opening.polls() == 1));
    files[42].opening.open();
    assert!(poll(&mut s).is_pending());
    for (i, f) in files.iter().enumerate() {
        assert_eq!(f.opening.polls(), if i == 42 { 2 } else { 1 }, "slot {i}");
    }
}
#[test]
fn ready_source_does_not_starve_other_sources() {
    let a = Read::new(0..10_000);
    let b = Read::new(10_000..10_002);
    let mut s = ReadScheduler::new(
        stream::iter([a, b.clone()]),
        FrameBudget::new(10_002),
        config(10_002, 20_000, 2, 0),
    );
    assert!(poll(&mut s).is_ready());
    assert_eq!(b.reading.polls(), 3);
}
#[test]
fn work_budget_yields_to_executor() {
    let f = Write::new(10_000);
    f.finalizing.close();
    let (input, drops) = frames(10_000);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([f]),
        FrameBudget::new(32),
        config(32, 10_000, 1, 0),
    );
    let (count, w) = context_waker();
    assert!(
        Pin::new(&mut s)
            .poll_next(&mut Context::from_waker(&w))
            .is_pending()
    );
    assert!(count.0.load(Ordering::Relaxed) > 0);
    assert!(drops.get() < 256);
}
#[test]
fn open_future_can_finish_long_before_first_io() {
    let f = Write::new(4);
    let mut s = WriteScheduler::new(
        stream::pending::<Frame>(),
        stream::iter([f.clone()]),
        FrameBudget::new(4),
        config(4, 10, 1, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(f.opening.polls(), 1);
    assert_eq!(f.writing.polls(), 0);
}
#[test]
fn blocked_consumer_does_not_prevent_allowed_readers_from_filling_window() {
    let a = Read::new(0..4);
    a.reading.close();
    let b = Read::new(4..8);
    let mut s = ReadScheduler::new(
        stream::iter([a, b.clone()]),
        FrameBudget::new(8),
        config(8, 100, 2, 0),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.reading.polls(), 5);
}
#[test]
fn blocked_writer_does_not_prevent_input_buffering_until_capacity() {
    let f = Write::new(100);
    f.writing.close();
    let (input, _) = frames(100);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(100),
        config(4, 100, 1, 0),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(s.retained_frames(), 4);
    assert_eq!(f.writing.polls(), 1);
    for _ in 0..10 {
        assert!(poll(&mut s).is_pending());
    }
    assert_eq!(f.writing.polls(), 1);
}
#[test]
fn eof_probe_waits_for_its_own_waker() {
    // A source can block specifically after its last data frame.
    struct File(Gate);
    struct Reader {
        emitted: bool,
        gate: Gate,
    }
    impl Stream for Reader {
        type Item = Result<usize, ()>;
        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let s = self.get_mut();
            if !s.emitted {
                s.emitted = true;
                Poll::Ready(Some(Ok(0)))
            } else if s.gate.ready(cx) {
                Poll::Ready(None)
            } else {
                Poll::Pending
            }
        }
    }
    impl io_scheduler::ReadFile<usize> for File {
        type Error = ();
        type Open = std::future::Ready<Result<Reader, ()>>;
        type Reader = Reader;
        fn frame_count(&self) -> u32 {
            1
        }
        fn open(&self) -> Self::Open {
            std::future::ready(Ok(Reader {
                emitted: false,
                gate: self.0.clone(),
            }))
        }
    }
    let gate = Gate::new(false);
    let mut s = ReadScheduler::new(
        stream::iter([File(gate.clone()), File(Gate::new(true))]),
        FrameBudget::new(2),
        config(2, 10, 2, 0),
    );
    assert_eq!(poll(&mut s), Poll::Ready(Some(Ok(0))));
    assert!(poll(&mut s).is_pending());
    assert_eq!(gate.polls(), 1);
    gate.open();
    assert_eq!(collect(s), [Ok(0)]);
}
#[test]
fn pending_input_is_not_repolled_when_writers_progress() {
    let (input, _) = frames(1);
    let gate = Gate::new(false);
    let g = gate.clone();
    let pending = stream::poll_fn(move |cx| {
        g.ready(cx);
        Poll::<Option<Frame>>::Pending
    });
    let f = Write::new(4);
    let mut s = WriteScheduler::new(
        input.chain(pending),
        stream::iter([f]),
        FrameBudget::new(4),
        config(4, 10, 1, 1),
    );
    assert!(poll(&mut s).is_pending());
    for _ in 0..5 {
        assert!(poll(&mut s).is_pending());
    }
    assert_eq!(gate.polls(), 1);
}
