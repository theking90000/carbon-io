//! Waker routing, bounded work, backpressure, and non-starvation tests.
mod support;
use carbon_io::FrameBudget;
use futures::{Stream, stream};
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
fn pending_fill_keeps_input_unconsumed_and_wakes_without_opening_file() {
    use carbon_io::{AsyncFilesWrite, WriteScheduler};
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 4, 1, 1);
    let mut allocator = Allocator::default();
    allocator.filling.close();
    let filling = allocator.filling.clone();
    let file = Write::new(4);
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file.clone()).unwrap();
    let (wakes, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut writer).poll_write(&cx, &7).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    assert_eq!(file.opens.get(), 0);
    for _ in 0..3 {
        assert!(Pin::new(&mut writer).poll_write(&cx, &7).is_pending());
    }
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    filling.open();
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    write_value(&mut writer, 7);
    assert_eq!(close_writer(&mut writer), [vec![7]]);
}

#[test]
fn cancelled_write_wait_preserves_attempt_and_replaces_backend_waker() {
    use carbon_io::{AsyncFilesWrite, WriteScheduler};
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let mut allocator = Allocator::default();
    let file = Write::new(2);
    file.writing.close();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file.clone()).unwrap();
    write_value(&mut writer, 0);
    let (old, old_waker) = context_waker();
    assert!(
        Pin::new(&mut writer)
            .poll_write(&Context::from_waker(&old_waker), &1)
            .is_pending()
    );
    let (new, new_waker) = context_waker();
    assert!(
        Pin::new(&mut writer)
            .poll_write(&Context::from_waker(&new_waker), &1)
            .is_pending()
    );
    let before = old.0.load(Ordering::Relaxed);
    file.writing.open();
    assert_eq!(old.0.load(Ordering::Relaxed), before);
    assert!(new.0.load(Ordering::Relaxed) > 0);
    write_value(&mut writer, 1);
    assert_eq!(file.opens.get(), 1);
    assert_eq!(close_writer(&mut writer), [vec![0, 1]]);
}
