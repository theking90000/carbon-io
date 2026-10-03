//! Waker routing, bounded work, backpressure, and non-starvation tests.
mod support;
use carbon_io::{FrameBudget};
use futures::{Stream, StreamExt, stream};
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
    let mut scheduler_files_1 = stream::iter([a.clone(), b.clone()]);
    let scheduler_budget_1 = FrameBudget::new(4);
    let mut scheduler_config_1 = config(4, 100, 10, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_1,
        &scheduler_budget_1,
        &mut scheduler_config_1,
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
    let mut scheduler_files_2 = stream::iter(files.clone());
    let scheduler_budget_2 = FrameBudget::new(32);
    let mut scheduler_config_2 = config(32, 20_000, 10_000, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_2,
        &scheduler_budget_2,
        &mut scheduler_config_2,
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
    let mut scheduler_files_3 = stream::iter([a, b.clone()]);
    let scheduler_budget_3 = FrameBudget::new(10_002);
    let mut scheduler_config_3 = config(10_002, 20_000, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_3,
        &scheduler_budget_3,
        &mut scheduler_config_3,
    );
    assert!(poll(&mut s).is_ready());
    assert_eq!(b.reading.polls(), 3);
}
#[test]
fn work_budget_yields_to_executor() {
    let f = Write::new(10_000);
    f.finalizing.close();
    let (input, drops) = frames(10_000);
    let mut scheduler_input_4 = input;
    let mut scheduler_files_4 = stream::iter([f]);
    let scheduler_budget_4 = FrameBudget::new(32);
    let mut scheduler_config_4 = config(32, 10_000, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_4,
        &mut scheduler_files_4,
        &scheduler_budget_4,
        &mut scheduler_config_4,
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
fn write_opens_when_pending_input_wakes_with_first_frame() {
    for retry in [0, 2] {
        let gate = Gate::new(false);
        let (mut input, _) = frames(1);
        let input_gate = gate.clone();
        let input = stream::poll_fn(move |cx| {
            if input_gate.ready(cx) {
                Pin::new(&mut input).poll_next(cx)
            } else {
                Poll::Pending
            }
        });
        let f = Write::new(4);
        let mut scheduler_input_5 = input;
        let mut scheduler_files_5 = stream::iter([f.clone()]);
        let scheduler_budget_5 = FrameBudget::new(4);
        let mut scheduler_config_5 = config(4, 10, 1, retry);
        let mut s = WriteDriver::new(
            &mut scheduler_input_5,
            &mut scheduler_files_5,
            &scheduler_budget_5,
            &mut scheduler_config_5,
        );
        let (wakes, waker) = context_waker();
        let mut cx = Context::from_waker(&waker);
        s.poll_progress(&mut cx);
        assert_eq!(f.opens.get(), 0);
        assert_eq!(f.opening.polls(), 0);
        let before = wakes.0.load(Ordering::Relaxed);
        s.poll_progress(&mut cx);
        assert_eq!(wakes.0.load(Ordering::Relaxed), before);
        gate.open();
        assert!(wakes.0.load(Ordering::Relaxed) > before);
        s.poll_progress(&mut cx);
        assert_eq!(f.opens.get(), 1);
        assert_eq!(collect(s), [Ok(vec![0])]);
    }
}
#[test]
fn blocked_consumer_does_not_prevent_allowed_readers_from_filling_window() {
    let a = Read::new(0..4);
    a.reading.close();
    let b = Read::new(4..8);
    let mut scheduler_files_6 = stream::iter([a, b.clone()]);
    let scheduler_budget_6 = FrameBudget::new(8);
    let mut scheduler_config_6 = config(8, 100, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_6,
        &scheduler_budget_6,
        &mut scheduler_config_6,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.reading.polls(), 5);
}
#[test]
fn blocked_writer_does_not_prevent_input_buffering_until_capacity() {
    let f = Write::new(100);
    f.writing.close();
    let (input, _) = frames(100);
    let mut scheduler_input_7 = input;
    let mut scheduler_files_7 = stream::iter([f.clone()]);
    let scheduler_budget_7 = FrameBudget::new(100);
    let mut scheduler_config_7 = config(4, 100, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_7,
        &mut scheduler_files_7,
        &scheduler_budget_7,
        &mut scheduler_config_7,
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
    impl carbon_io::ReadFile<usize> for File {
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
    let mut scheduler_files_8 =
        std::pin::pin!(stream::iter([File(gate.clone()), File(Gate::new(true))]));
    let scheduler_budget_8 = FrameBudget::new(2);
    let mut scheduler_config_8 = config(2, 10, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_8,
        &scheduler_budget_8,
        &mut scheduler_config_8,
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
    let mut scheduler_input_9 = input.chain(pending);
    let mut scheduler_files_9 = stream::iter([f]);
    let scheduler_budget_9 = FrameBudget::new(4);
    let mut scheduler_config_9 = config(4, 10, 1, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_9,
        &mut scheduler_files_9,
        &scheduler_budget_9,
        &mut scheduler_config_9,
    );
    assert!(poll(&mut s).is_pending());
    for _ in 0..5 {
        assert!(poll(&mut s).is_pending());
    }
    assert_eq!(gate.polls(), 1);
}
