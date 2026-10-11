//! Direct wakers, bounded polling, backpressure, and write cancellation.
mod support;
use carbon_io::{FrameBudget, ReadFile, ReadScheduler};
use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn pending_files_are_polled_once_per_call_without_self_waking() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 100, 25, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let files: Vec<_> = (0..25)
        .map(|i| {
            let f = Read::new([i]);
            f.opening.close();
            f
        })
        .collect();
    for file in &files {
        assert!(reader.push_file(file.clone()).is_ok());
    }
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    reader.poll_progress(&mut cx);
    assert!(files.iter().all(|file| file.opening.polls() == 1));
    let before = wakes.0.load(Ordering::Relaxed);
    reader.poll_progress(&mut cx);
    assert!(files.iter().all(|file| file.opening.polls() == 2));
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    files[0].opening.open();
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    assert_eq!(poll_read(&mut reader), Poll::Ready(Some(Ok(0))));
}

#[test]
fn pending_backend_waker_is_replaced_by_the_next_call() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 2, 1, 0);
    let file = Read::new([0]);
    file.reading.close();
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(reader.push_file(file.clone()).is_ok());
    let (old, old_waker) = context_waker();
    reader.poll_progress(&mut Context::from_waker(&old_waker));
    let (new, new_waker) = context_waker();
    reader.poll_progress(&mut Context::from_waker(&new_waker));
    let before = old.0.load(Ordering::Relaxed);
    file.reading.open();
    assert_eq!(old.0.load(Ordering::Relaxed), before);
    assert!(new.0.load(Ordering::Relaxed) > 0);
}

#[test]
fn declared_count_does_not_poll_eof_or_block_following_output() {
    struct File {
        emitted: bool,
        gate: Gate,
    }
    impl ReadFile<usize> for File {
        type Error = ();
        fn frame_count(&self) -> u32 {
            1
        }
        fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            frame: &mut usize,
        ) -> Poll<Result<Option<()>, ()>> {
            let this = self.get_mut();
            if !this.emitted {
                this.emitted = true;
                *frame = 0;
                return Poll::Ready(Ok(Some(())));
            }
            if this.gate.ready(cx) {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            }
        }
    }
    let gate = Gate::new(false);
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 10, 2, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(
        reader
            .push_file(File {
                emitted: false,
                gate: gate.clone()
            })
            .is_ok()
    );
    assert!(
        reader
            .push_file(File {
                emitted: false,
                gate: Gate::new(true)
            })
            .is_ok()
    );
    reader.close_files();
    assert_eq!(poll_read(&mut reader), Poll::Ready(Some(Ok(0))));
    assert_eq!(collect_read(&mut reader), [Ok(0)]);
    assert_eq!(gate.polls(), 0);
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
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut writer).poll_write(&mut cx, &7).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    assert_eq!(file.opens.get(), 0);
    for _ in 0..3 {
        assert!(Pin::new(&mut writer).poll_write(&mut cx, &7).is_pending());
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
            .poll_write(&mut Context::from_waker(&old_waker), &1)
            .is_pending()
    );
    let (new, new_waker) = context_waker();
    assert!(
        Pin::new(&mut writer)
            .poll_write(&mut Context::from_waker(&new_waker), &1)
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

#[test]
fn configured_window_above_256_can_register_all_pending_files_without_busy_loop() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 400, 300, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let files: Vec<_> = (0..300)
        .map(|i| {
            let file = Read::new([i]);
            file.opening.close();
            file
        })
        .collect();
    for file in &files {
        assert!(reader.push_file(file.clone()).is_ok());
    }
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    reader.poll_progress(&mut cx);
    assert!(files.iter().all(|file| file.opening.polls() == 1));
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
}
