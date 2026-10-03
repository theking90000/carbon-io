//! Shared capacity, reservation, wakeup, and cancellation regressions.
mod support;
use carbon_io::{ContractError, FrameBudget};
use futures::Stream;
use futures::stream;
use std::{
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
};
use support::*;

#[test]
fn invalid_growth_requests_preserve_grants_and_fifo_waits() {
    let budget = FrameBudget::new(8);
    let mut owner = budget.permit();
    let mut waiting = budget.permit();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(owner.poll_grow(&mut cx, 8, 8), Poll::Ready(Ok(())));
    assert!(waiting.poll_grow(&mut cx, 4, 8).is_pending());
    for (minimum, desired) in [(9, 8), (8, 9)] {
        assert_eq!(
            waiting.poll_grow(&mut cx, minimum, desired),
            Poll::Ready(Err(ContractError::InvalidBudgetRequest))
        );
    }
    drop(owner);
    assert_eq!(budget.available_capacity(), 0);
    assert_eq!(
        waiting.poll_grow(&mut cx, 9, 9),
        Poll::Ready(Err(ContractError::InvalidBudgetRequest))
    );
    assert_eq!(waiting.poll_grow(&mut cx, 4, 8), Poll::Ready(Ok(())));
    assert_eq!(
        waiting.poll_grow(&mut cx, 2, 1),
        Poll::Ready(Err(ContractError::InvalidBudgetRequest))
    );
    assert_eq!(waiting.capacity(), 8);
    drop(waiting);
    assert_eq!(budget.available_capacity(), 8);
}

#[test]
fn shared_budget_never_overallocates() {
    let b = FrameBudget::new(64);
    let mut a = b.permit();
    let mut c = b.permit();
    let (_, w) = context_waker();
    let mut cx = Context::from_waker(&w);
    assert!(a.poll_grow(&mut cx, 32, 48).is_ready());
    assert!(c.poll_grow(&mut cx, 32, 48).is_pending());
    assert_eq!(a.capacity(), 48);
    assert_eq!(c.capacity(), 0);
    assert_eq!(b.available_capacity(), 16);
    a.shrink_to(0);
    assert!(c.poll_grow(&mut cx, 32, 48).is_ready());
    assert_eq!(c.capacity(), 48);
}
#[test]
fn drop_returns_all_capacity() {
    let b = FrameBudget::new(8);
    let mut p = b.permit();
    let (_, w) = context_waker();
    assert!(p.poll_grow(&mut Context::from_waker(&w), 8, 8).is_ready());
    drop(p);
    assert_eq!(b.available_capacity(), 8);
}
#[test]
fn waiting_scheduler_is_woken_when_capacity_returns() {
    let b = FrameBudget::new(8);
    let mut a = b.permit();
    let mut c = b.permit();
    let (count, w) = context_waker();
    let mut cx = Context::from_waker(&w);
    assert!(a.poll_grow(&mut cx, 8, 8).is_ready());
    assert!(c.poll_grow(&mut cx, 8, 8).is_pending());
    drop(a);
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    assert_eq!(b.available_capacity(), 0);
    assert!(c.poll_grow(&mut cx, 8, 8).is_ready());
}
#[test]
fn wakes_only_waiters_whose_minimum_has_been_reserved() {
    let b = FrameBudget::new(100);
    let mut owner = b.permit();
    let (_, w) = context_waker();
    assert!(
        owner
            .poll_grow(&mut Context::from_waker(&w), 100, 100)
            .is_ready()
    );
    let mut waiters: Vec<_> = (0..5).map(|_| (b.permit(), context_waker())).collect();
    for (p, (_, w)) in &mut waiters {
        assert!(
            p.poll_grow(&mut Context::from_waker(w), 10, 10)
                .is_pending()
        );
    }
    owner.shrink_to(95);
    assert!(
        waiters
            .iter()
            .all(|(_, (c, _))| c.0.load(Ordering::Relaxed) == 0)
    );
    owner.shrink_to(90);
    assert_eq!(waiters[0].1.0.0.load(Ordering::Relaxed), 1);
    assert!(
        waiters[1..]
            .iter()
            .all(|(_, (c, _))| c.0.load(Ordering::Relaxed) == 0)
    );
    assert_eq!(b.available_capacity(), 0);
}
#[test]
fn canceled_or_unclaimed_grants_do_not_leak() {
    let b = FrameBudget::new(8);
    let (_, w) = context_waker();
    let mut cx = Context::from_waker(&w);
    let mut owner = b.permit();
    let mut first = b.permit();
    let mut second = b.permit();
    assert!(owner.poll_grow(&mut cx, 8, 8).is_ready());
    assert!(first.poll_grow(&mut cx, 8, 8).is_pending());
    assert!(second.poll_grow(&mut cx, 8, 8).is_pending());
    drop(owner);
    drop(first);
    assert!(second.poll_grow(&mut cx, 8, 8).is_ready());
    drop(second);
    assert_eq!(b.available_capacity(), 8);
}
#[test]
fn scheduler_can_grow_when_capacity_is_released() {
    let b = FrameBudget::new(128);
    let (_, w) = context_waker();
    let mut owner = b.permit();
    assert!(
        owner
            .poll_grow(&mut Context::from_waker(&w), 96, 96)
            .is_ready()
    );
    let file = Read::new(0..256);
    file.reading.close();
    let mut scheduler_files_1 = stream::iter([file]);
    let mut scheduler_config_1 = config(128, 256, 1, 0);
    let mut reader = ReadDriver::new(&mut scheduler_files_1, &b, &mut scheduler_config_1);
    assert!(poll(&mut reader).is_pending());
    assert_eq!(reader.granted_frames(), 32);
    drop(owner);
    assert!(poll(&mut reader).is_pending());
    assert_eq!(reader.granted_frames(), 128);
}
#[test]
fn scheduler_can_shrink() {
    let b = FrameBudget::new(64);
    let file = Read::new(0..128);
    let mut scheduler_files_2 = stream::iter([file]);
    let mut scheduler_config_2 = config(64, 128, 1, 0);
    let mut r = ReadDriver::new(&mut scheduler_files_2, &b, &mut scheduler_config_2);
    assert!(poll(&mut r).is_ready());
    r.set_window(config(4, 128, 1, 0).window).unwrap();
    for _ in 0..63 {
        assert!(poll(&mut r).is_ready());
    }
    assert_eq!(r.granted_frames(), 4);
    assert_eq!(b.available_capacity(), 60);
}
#[test]
fn multiple_read_and_write_schedulers_share_budget() {
    let b = FrameBudget::new(8);
    let rfile = Read::new(0..4);
    rfile.reading.close();
    let wfile = Write::new(4);
    wfile.writing.close();
    let (input, _) = frames(4);
    let mut scheduler_files_3 = stream::iter([rfile]);
    let mut scheduler_config_3 = config(4, 4, 1, 0);
    let mut r = ReadDriver::new(&mut scheduler_files_3, &b, &mut scheduler_config_3);
    let mut scheduler_input_4 = input;
    let mut scheduler_files_4 = stream::iter([wfile]);
    let mut scheduler_config_4 = config(4, 4, 1, 1);
    let mut w = WriteDriver::new(
        &mut scheduler_input_4,
        &mut scheduler_files_4,
        &b,
        &mut scheduler_config_4,
    );
    assert!(poll(&mut r).is_pending());
    assert!(poll(&mut w).is_pending());
    assert_eq!(r.granted_frames() + w.granted_frames(), 8);
    drop(r);
    drop(w);
    assert_eq!(b.available_capacity(), 8);
}
#[test]
fn two_replay_writers_do_not_deadlock_with_partial_local_grants() {
    let b = FrameBudget::new(10);
    let a = Write::new(6);
    a.finalizing.close();
    let c = Write::new(6);
    let (i1, _) = frames(6);
    let (i2, _) = frames(6);
    let mut scheduler_input_5 = i1;
    let mut scheduler_files_5 = stream::iter([a.clone()]);
    let mut scheduler_config_5 = config(8, 10, 1, 1);
    let mut s1 = WriteDriver::new(
        &mut scheduler_input_5,
        &mut scheduler_files_5,
        &b,
        &mut scheduler_config_5,
    );
    let mut scheduler_input_6 = i2;
    let mut scheduler_files_6 = stream::iter([c]);
    let mut scheduler_config_6 = config(8, 10, 1, 1);
    let mut s2 = WriteDriver::new(
        &mut scheduler_input_6,
        &mut scheduler_files_6,
        &b,
        &mut scheduler_config_6,
    );
    assert!(poll(&mut s1).is_pending());
    assert!(poll(&mut s2).is_pending());
    assert_eq!(s2.retained_frames(), 0);
    a.finalizing.open();
    assert_eq!(collect(s1), [Ok((0..6).collect())]);
    assert_eq!(collect(s2), [Ok((0..6).collect())]);
    assert_eq!(b.available_capacity(), 10);
}
#[test]
fn small_releases_do_not_cause_unit_grants() {
    let b = FrameBudget::new(128);
    let mut owner = b.permit();
    let (_, w) = context_waker();
    assert!(
        owner
            .poll_grow(&mut Context::from_waker(&w), 128, 128)
            .is_ready()
    );
    let f = Read::new(0..128);
    f.reading.close();
    let mut scheduler_files_7 = stream::iter([f]);
    let mut scheduler_config_7 = config(128, 128, 1, 0);
    let mut s = ReadDriver::new(&mut scheduler_files_7, &b, &mut scheduler_config_7);
    let (c, w) = context_waker();
    let mut cx = Context::from_waker(&w);
    assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
    let before = c.0.load(Ordering::Relaxed);
    for n in 1..32 {
        owner.shrink_to(128 - n);
        assert_eq!(c.0.load(Ordering::Relaxed), before);
    }
    owner.shrink_to(96);
    assert!(c.0.load(Ordering::Relaxed) > before);
    assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
    assert_eq!(s.granted_frames(), 32);
}
#[test]
fn concurrent_permits_conserve_capacity() {
    let b = FrameBudget::new(64);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let b = b.clone();
            scope.spawn(move || {
                let wake = Arc::new(Counter::default());
                let w = std::task::Waker::from(wake);
                let mut cx = Context::from_waker(&w);
                for _ in 0..200 {
                    let mut p = b.permit();
                    while p.poll_grow(&mut cx, 8, 8).is_pending() {
                        std::thread::yield_now();
                    }
                    assert_eq!(p.capacity(), 8);
                }
            });
        }
    });
    assert_eq!(b.available_capacity(), 64);
}
