//! Direct polling, bounded work, and deferred errors replace read events.
mod support;
use carbon_io::{FrameBudget, ReadScheduler, SchedulerError};
use std::{
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn progress_resumes_from_budget_wake_with_bounded_backend_work() {
    let budget = FrameBudget::new(1024);
    let mut held = budget.permit();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(held.poll_grow(&mut cx, 1024, 1024).is_ready());
    let file = Read::new(0..1024);
    let mut cfg = config(1024, 1024, 1, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(reader.push_file(file.clone()).is_ok());
    reader.close_files();
    reader.poll_progress(&mut cx);
    assert_eq!(file.reading.polls(), 0);
    let before = wakes.0.load(Ordering::Relaxed);
    drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    reader.poll_progress(&mut cx);
    assert!(file.reading.polls() > 0 && file.reading.polls() <= 256);
    assert_eq!(
        collect_read(&mut reader),
        (0..1024).map(Ok).collect::<Vec<_>>()
    );
}

#[test]
fn progress_retains_terminal_error_despite_refused_admission() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 8, 2, 0);
    let mut file = Read::new([0]);
    file.values = vec![Err("read")];
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(reader.push_file(file).is_ok());
    let (_, waker) = context_waker();
    reader.poll_progress(&mut Context::from_waker(&waker));
    assert!(reader.push_file(Read::new([1])).is_err());
    reader.close_files();
    assert_eq!(
        poll_read(&mut reader),
        Poll::Ready(Some(Err(SchedulerError::Backend("read"))))
    );
    assert_eq!(poll_read(&mut reader), Poll::Ready(None));
    assert_eq!(budget.available_capacity(), 2);
}
