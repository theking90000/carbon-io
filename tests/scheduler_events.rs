//! Combined scheduler events, fairness, cancellation, and bounded progress.
mod support;

use carbon_io::{ContractError, FrameBudget, Interest, ReadEvent, ReadScheduler, SchedulerError};
use futures::executor::block_on;
use std::{
    collections::VecDeque,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn ignored_read_admission_waits_and_external_enqueue_wakes_once() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 8, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::FileReady)))
    );
    let before = wakes.0.load(Ordering::Relaxed);
    for _ in 0..8 {
        assert!(
            reader
                .poll_with_interest(&mut cx, Interest::RESULTS)
                .is_pending()
        );
    }
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    // No producer Context is needed: the last waiting task is still notified.
    reader.enqueue_file(Read::new(0..1)).unwrap();
    assert!(reader.accept_file());
    reader.enqueue_file(Read::new(1..2)).unwrap();
    reader.close_files().unwrap();
    assert_eq!(wakes.0.load(Ordering::Relaxed), before + 1);
    assert_eq!(collect(reader), [Ok(0), Ok(1)]);
}

#[test]
fn one_read_notification_admits_files_until_the_horizon_or_active_limit() {
    for (ahead, active) in [(3, 10), (20, 2)] {
        let budget = FrameBudget::new(2);
        let mut cfg = config(2, ahead, active, 0);
        let mut reader = ReadScheduler::new(&budget, &mut cfg);
        let descriptors = [Read::new(0..2), Read::new(2..4), Read::new(4..6)];
        let mut files = VecDeque::from(descriptors.clone());

        assert_eq!(
            block_on(reader.next_event()),
            Ok(Some(ReadEvent::FileReady))
        );
        while !files.is_empty() && reader.accept_file() {
            assert!(
                reader.accept_file(),
                "checking twice preserves the same reservation"
            );
            reader.enqueue_file(files.pop_front().unwrap()).unwrap();
        }
        assert_eq!(files.len(), 1);
        assert!(!reader.accept_file());
        assert_eq!(descriptors[0].opens.get(), 1);
        assert_eq!(descriptors[1].opens.get(), 1);
        assert_eq!(descriptors[2].opens.get(), 0);
        for file in &descriptors {
            assert_eq!(file.opening.polls(), 0);
            assert_eq!(file.reading.polls(), 0);
        }

        reader.close_files().unwrap();
        assert!(!reader.accept_file());
        assert_eq!(collect(reader), (0..4).map(Ok).collect::<Vec<_>>());
    }
}

#[test]
fn synchronous_admission_checks_preserve_errors_for_the_next_poll() {
    let budget = FrameBudget::new(0);
    let mut read_cfg = config(1, 1, 1, 0);
    let mut reader = ReadScheduler::<usize, Read>::new(&budget, &mut read_cfg);

    assert!(!reader.accept_file());
    assert_eq!(
        block_on(reader.next_event()),
        Err(SchedulerError::Contract(ContractError::ZeroBudget))
    );
    assert!(!reader.accept_file());
    assert_eq!(block_on(reader.next_event()), Ok(None));
}

#[test]
fn read_events_alternate_admission_and_output_without_losing_grants() {
    let budget = FrameBudget::new(3);
    let mut cfg = config(3, 12, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::FileReady)))
    );
    reader.enqueue_file(Read::new(0..3)).unwrap();
    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::Frame(0))))
    );
    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::FileReady)))
    );
    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::Frame(1))))
    );
    block_on(reader.file_ready()).unwrap();
    reader.enqueue_file(Read::new(3..6)).unwrap();
    reader.close_files().unwrap();

    let mut values = vec![0, 1];
    while let Some(event) = block_on(reader.next_event()).unwrap() {
        match event {
            ReadEvent::Frame(frame) => values.push(frame),
            ReadEvent::FileReady => panic!("closed file input emitted readiness"),
        }
    }
    assert_eq!(values, [0, 1, 2, 3, 4, 5]);
    assert_eq!(budget.available_capacity(), 3);
}

#[test]
fn read_event_poll_resumes_from_budget_wake_with_bounded_work() {
    let budget = FrameBudget::new(1024);
    let mut held = budget.permit();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(held.poll_grow(&mut cx, 1024, 1024).is_ready());
    let file = Read::new(0..1024);
    let mut cfg = config(1024, 1024, 1, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::FileReady)))
    );
    reader.enqueue_file(file.clone()).unwrap();
    reader.close_files().unwrap();
    assert!(reader.poll(&mut cx).is_pending());
    assert_eq!(file.reading.polls(), 0);
    let before = wakes.0.load(Ordering::Relaxed);
    drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(
        reader.poll(&mut cx),
        Poll::Ready(Ok(Some(ReadEvent::Frame(0))))
    );
    assert!(file.reading.polls() > 0 && file.reading.polls() <= 256);
    assert_eq!(collect(reader), (1..1024).map(Ok).collect::<Vec<_>>());
}

#[test]
fn event_errors_are_reported_once_then_end() {
    let budget = FrameBudget::new(0);
    let mut read_cfg = config(1, 1, 1, 0);
    let mut reader = ReadScheduler::<usize, Read>::new(&budget, &mut read_cfg);
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        reader.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Err(SchedulerError::Contract(ContractError::ZeroBudget)))
    );
    assert_eq!(
        reader.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Ok(None))
    );
}
