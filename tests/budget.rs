//! Shared capacity, reservation, wakeup, and cancellation regressions.
mod support;
use futures::Stream;
use futures::stream;
use io_scheduler::{FrameBudget, ReadScheduler, WriteScheduler};
use std::{
    pin::Pin,
    sync::{Arc, atomic::Ordering},
    task::Context,
};
use support::*;

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
    let mut reader = ReadScheduler::new(stream::iter([file]), b.clone(), config(128, 256, 1, 0));
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
    let mut r = ReadScheduler::new(stream::iter([file]), b.clone(), config(64, 128, 1, 0));
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
    let mut r = ReadScheduler::new(stream::iter([rfile]), b.clone(), config(4, 4, 1, 0));
    let mut w = WriteScheduler::new(input, stream::iter([wfile]), b.clone(), config(4, 4, 1, 1));
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
    let mut s1 = WriteScheduler::new(
        i1,
        stream::iter([a.clone()]),
        b.clone(),
        config(8, 10, 1, 1),
    );
    let mut s2 = WriteScheduler::new(i2, stream::iter([c]), b.clone(), config(8, 10, 1, 1));
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
    let mut s = ReadScheduler::new(stream::iter([f]), b.clone(), config(128, 128, 1, 0));
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
