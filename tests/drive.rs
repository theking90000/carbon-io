//! Driving under consumer backpressure, cancellation, and deferred errors.
mod support;

use futures::{FutureExt, StreamExt, stream, stream::FusedStream};
use io_scheduler::{ContractError, FrameBudget, ReadScheduler, SchedulerError, WriteScheduler};
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
    let mut reader = ReadScheduler::new(
        stream::iter([file.clone()]),
        FrameBudget::new(4),
        config(4, 4, 1, 0),
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
fn writes_finish_under_backpressure_but_results_stay_bounded_and_ordered() {
    let files: Vec<_> = (0..5).map(|_| Write::new(2)).collect();
    files[0].finalizing.close();
    let (input, _) = frames(10);
    let mut writer = WriteScheduler::new(
        input,
        stream::iter(files.clone()),
        FrameBudget::new(4),
        config(4, 4, 2, 0),
    );
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    {
        let mut drive = Box::pin(writer.progress());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
        assert_eq!(files[0].finalized.get(), 0);
        assert_eq!(files[1].finalized.get(), 1);
        let before = wakes.0.load(Ordering::Relaxed);
        files[0].finalizing.open();
        assert!(wakes.0.load(Ordering::Relaxed) > before);
        assert!(drive.as_mut().poll(&mut cx).is_pending());
        assert_eq!(files[0].finalized.get(), 1);
        let before = wakes.0.load(Ordering::Relaxed);
        for _ in 0..3 {
            assert!(drive.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(wakes.0.load(Ordering::Relaxed), before);
        assert!(files[2..].iter().all(|f| f.attempts.borrow().is_empty()));
    }
    assert_eq!(writer.retained_frames(), 0);
    assert_eq!(
        collect(writer),
        (0..5)
            .map(|i| Ok(vec![i * 2, i * 2 + 1]))
            .collect::<Vec<_>>()
    );
}

#[test]
fn cancelling_drive_and_next_preserves_pending_io_and_replaces_waker() {
    let file = Read::new(0..2);
    file.opening.close();
    let budget = FrameBudget::new(2);
    let mut reader = ReadScheduler::new(
        stream::iter([file.clone()]),
        budget.clone(),
        config(2, 2, 1, 0),
    );
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
fn cancelling_write_drive_preserves_attempt_and_scheduler_drop_releases_frames() {
    let file = Write::new(10);
    file.writing.close();
    let (input, drops) = frames(10);
    let budget = FrameBudget::new(4);
    let mut writer = WriteScheduler::new(
        input,
        stream::iter([file.clone()]),
        budget.clone(),
        config(4, 4, 1, 0),
    );
    let (_, waker) = context_waker();
    {
        let mut drive = Box::pin(writer.progress());
        assert!(
            drive
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    assert_eq!(writer.retained_frames(), 4);
    assert_eq!(drops.get(), 0);
    assert_eq!(file.drops.get(), 0);
    assert!(poll(&mut writer).is_pending());
    assert_eq!(file.attempts.borrow().len(), 1);
    drop(writer);
    assert_eq!(drops.get(), 10);
    assert_eq!(file.drops.get(), 1);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn read_drive_releases_resources_on_error_but_delivers_error_once() {
    let mut file = Read::new(0..3);
    file.values[1] = Err("read");
    let budget = FrameBudget::new(3);
    let mut reader = ReadScheduler::new(
        stream::iter([file.clone()]),
        budget.clone(),
        config(3, 3, 1, 0),
    );
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
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        poll(&mut reader),
        Poll::Ready(Some(Err(SchedulerError::Backend("read"))))
    );
    assert!(reader.is_terminated());
    assert_eq!(poll(&mut reader), Poll::Ready(None));
    reader.poll_progress(&mut cx);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
}

#[test]
fn write_drive_releases_resources_on_error_but_delivers_error_once() {
    let file = Write::new(3);
    file.finalize_failures.set(1);
    let budget = FrameBudget::new(3);
    let (input, drops) = frames(3);
    let mut writer = WriteScheduler::new(
        input,
        stream::iter([file.clone()]),
        budget.clone(),
        config(3, 3, 1, 0),
    );
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    {
        let mut drive = Box::pin(writer.progress());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
        assert!(drive.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(drops.get(), 3);
    assert_eq!(budget.available_capacity(), 3);
    assert!(!writer.is_terminated());
    assert_eq!(
        poll(&mut writer),
        Poll::Ready(Some(Err(SchedulerError::Backend("finalize"))))
    );
    assert!(writer.is_terminated());
    assert_eq!(poll(&mut writer), Poll::Ready(None));
    writer.poll_progress(&mut cx);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
}

#[test]
fn progress_handles_initial_errors_and_empty_streams() {
    for capacity in [0, 1] {
        let budget = FrameBudget::new(capacity);
        let mut reader = ReadScheduler::<usize, _>::new(
            stream::empty::<Read>(),
            budget.clone(),
            config(1, 1, 1, 0),
        );
        let mut writer = WriteScheduler::new(
            stream::empty::<Frame>(),
            stream::empty::<Write>(),
            budget,
            config(1, 1, 1, 0),
        );
        let (wakes, waker) = context_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..2 {
            reader.poll_progress(&mut cx);
            writer.poll_progress(&mut cx);
        }
        if capacity == 0 {
            assert!(!reader.is_terminated());
            assert!(!writer.is_terminated());
            assert_eq!(
                poll(&mut reader),
                Poll::Ready(Some(Err(SchedulerError::Contract(
                    ContractError::ZeroBudget
                ))))
            );
            assert_eq!(
                poll(&mut writer),
                Poll::Ready(Some(Err(SchedulerError::Contract(
                    ContractError::ZeroBudget
                ))))
            );
        }
        assert!(reader.is_terminated());
        assert!(writer.is_terminated());
        assert_eq!(poll(&mut reader), Poll::Ready(None));
        assert_eq!(poll(&mut writer), Poll::Ready(None));
        assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn progress_yields_after_bounded_work_and_can_resume_from_budget_wake() {
    let budget = FrameBudget::new(1024);
    let mut held = budget.permit();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(held.poll_grow(&mut cx, 1024, 1024).is_ready());
    let file = Read::new(0..1024);
    let mut reader = ReadScheduler::new(
        stream::iter([file.clone()]),
        budget,
        config(1024, 1024, 1, 0),
    );
    reader.poll_progress(&mut cx);
    assert_eq!(file.reading.polls(), 0);
    drop(held);
    let before = wakes.0.load(Ordering::Relaxed);
    reader.poll_progress(&mut cx);
    assert!(file.reading.polls() > 0 && file.reading.polls() <= 256);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(collect(reader).len(), 1024);

    let file = Write::new(1024);
    let (input, _) = frames(1024);
    let mut writer = WriteScheduler::new(
        input,
        stream::iter([file.clone()]),
        FrameBudget::new(4),
        config(4, 4, 1, 0),
    );
    let before = wakes.0.load(Ordering::Relaxed);
    writer.poll_progress(&mut cx);
    assert!(file.writing.polls() > 0 && file.writing.polls() <= 256);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(collect(writer), [Ok((0..1024).collect())]);
}
