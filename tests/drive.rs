//! Driving under consumer backpressure, cancellation, and deferred errors.
mod support;

use carbon_io::{FrameBudget, ReadScheduler, SchedulerError};
use futures::{FutureExt, StreamExt, stream, stream::FusedStream};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn select_drives_reads_until_window_full_and_preserves_output() {
    let file = Read::new(0..20);
    file.reading.close();
    let mut scheduler_files_1 = stream::iter([file.clone()]);
    let scheduler_budget_1 = FrameBudget::new(4);
    let mut scheduler_config_1 = config(4, 4, 1, 0);
    let mut reader = ReadDriver::new(
        &mut scheduler_files_1,
        &scheduler_budget_1,
        &mut scheduler_config_1,
    );
    let consumer = Gate::new(false);
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    {
        let mut selected = Box::pin(async {
            futures::select_biased! {
                _ = reader.progress().fuse() => panic!("progress must never complete"),
                value = poll_fn(|cx| {
                    if consumer.ready(cx) { Poll::Ready(42) } else { Poll::Pending }
                }).fuse() => value,
            }
        });
        assert!(selected.as_mut().poll(&mut cx).is_pending());
        assert_eq!(file.reading.polls(), 1);
        let before = wakes.0.load(Ordering::Relaxed);
        file.reading.open();
        assert!(wakes.0.load(Ordering::Relaxed) > before);
        assert!(selected.as_mut().poll(&mut cx).is_pending());
        assert_eq!(file.reading.polls(), 5); // One blocked poll, then four frames.
        let before = wakes.0.load(Ordering::Relaxed);
        for _ in 0..3 {
            assert!(selected.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(file.reading.polls(), 5);
        assert_eq!(wakes.0.load(Ordering::Relaxed), before);
        consumer.open();
        assert_eq!(selected.as_mut().poll(&mut cx), Poll::Ready(42));
    }
    assert_eq!(reader.granted_frames(), 4);
    assert_eq!(collect(reader), (0..20).map(Ok).collect::<Vec<_>>());
}

#[test]
fn cancelling_drive_and_next_preserves_pending_io_and_replaces_waker() {
    let file = Read::new(0..2);
    file.opening.close();
    let budget = FrameBudget::new(2);
    let mut scheduler_files_3 = stream::iter([file.clone()]);
    let mut scheduler_config_3 = config(2, 2, 1, 0);
    let mut reader = ReadDriver::new(&mut scheduler_files_3, &budget, &mut scheduler_config_3);
    let (old_wakes, old_waker) = context_waker();
    {
        let mut drive = Box::pin(reader.progress());
        assert!(
            drive
                .as_mut()
                .poll(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
    }
    let (new_wakes, new_waker) = context_waker();
    {
        let mut next = reader.next();
        assert!(
            Pin::new(&mut next)
                .poll(&mut Context::from_waker(&new_waker))
                .is_pending()
        );
    }
    let before = old_wakes.0.load(Ordering::Relaxed);
    file.opening.open();
    assert_eq!(old_wakes.0.load(Ordering::Relaxed), before);
    assert!(new_wakes.0.load(Ordering::Relaxed) > 0);
    assert_eq!(file.opens.get(), 1);
    assert_eq!(collect(reader), [Ok(0), Ok(1)]);
    assert_eq!(budget.available_capacity(), 2);
}

#[test]
fn read_drive_releases_resources_on_error_but_delivers_error_once() {
    let mut file = Read::new(0..3);
    file.values[1] = Err("read");
    let budget = FrameBudget::new(3);
    let mut scheduler_files_5 = stream::iter([file.clone()]);
    let mut scheduler_config_5 = config(3, 3, 1, 0);
    let mut reader = ReadDriver::new(&mut scheduler_files_5, &budget, &mut scheduler_config_5);
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    {
        let mut drive = Box::pin(reader.progress());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(budget.available_capacity(), 3);
    assert_eq!(file.drops.get(), 1);
    assert!(!reader.is_terminated());
    let before = wakes.0.load(Ordering::Relaxed);
    reader.poll_progress(&mut cx);
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    assert_eq!(
        poll(&mut reader),
        Poll::Ready(Some(Err(SchedulerError::Backend("read"))))
    );
    assert!(reader.is_terminated());
    assert_eq!(poll(&mut reader), Poll::Ready(None));
    reader.poll_progress(&mut cx);
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
}

#[test]
fn progress_yields_after_bounded_work_and_can_resume_from_budget_wake() {
    let budget = FrameBudget::new(1024);
    let mut held = budget.permit();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(held.poll_grow(&mut cx, 1024, 1024).is_ready());
    let file = Read::new(0..1024);
    let mut scheduler_config_9 = config(1024, 1024, 1, 0);
    let mut reader = ReadScheduler::new(&budget, &mut scheduler_config_9);
    assert_eq!(
        Pin::new(&mut reader).poll_file_ready(&mut cx),
        Poll::Ready(Ok(()))
    );
    Pin::new(&mut reader).enqueue_file(file.clone()).unwrap();
    Pin::new(&mut reader).close_files().unwrap();
    reader.poll_progress(&mut cx);
    assert_eq!(file.reading.polls(), 0);
    let before = wakes.0.load(Ordering::Relaxed);
    drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    let before = wakes.0.load(Ordering::Relaxed);
    reader.poll_progress(&mut cx);
    assert!(file.reading.polls() > 0 && file.reading.polls() <= 256);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(collect(reader).len(), 1024);
}

#[test]
fn read_progress_handles_initial_errors_and_empty_streams() {
    for capacity in [0, 1] {
        let budget = FrameBudget::new(capacity);
        let mut files = stream::empty::<Read>();
        let mut cfg = config(1, 1, 1, 0);
        let mut reader = ReadDriver::<usize, _>::new(&mut files, &budget, &mut cfg);
        let (wakes, waker) = context_waker();
        let mut cx = Context::from_waker(&waker);
        reader.poll_progress(&mut cx);
        let before = wakes.0.load(Ordering::Relaxed);
        for _ in 0..2 {
            reader.poll_progress(&mut cx);
        }
        assert_eq!(wakes.0.load(Ordering::Relaxed), before);
        if capacity == 0 {
            assert_eq!(
                poll(&mut reader),
                Poll::Ready(Some(Err(SchedulerError::Contract(
                    carbon_io::ContractError::ZeroBudget
                ))))
            );
        }
        assert!(reader.is_terminated());
        assert_eq!(poll(&mut reader), Poll::Ready(None));
    }
}

#[test]
fn write_poll_yields_after_bounded_backend_work_and_resumes() {
    use carbon_io::{AsyncFilesWrite, WriteScheduler};
    let budget = FrameBudget::new(1024);
    let mut cfg = config(1024, 1024, 1, 0);
    let mut allocator = Allocator::default();
    let file = Write::new(1024);
    file.writing.close();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file.clone()).unwrap();
    for value in 0..1024 {
        write_value(&mut writer, value);
    }
    file.writing.open();
    let (wakes, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    let before_polls = file.writing.polls();
    let before_wakes = wakes.0.load(Ordering::Relaxed);
    assert!(Pin::new(&mut writer).poll_close(&cx).is_pending());
    let polls = file.writing.polls() - before_polls;
    assert!(polls > 0 && polls <= 256);
    assert!(wakes.0.load(Ordering::Relaxed) > before_wakes);
    assert_eq!(close_writer(&mut writer), [(0..1024).collect::<Vec<_>>()]);
    assert_eq!(budget.available_capacity(), 1024);
}
