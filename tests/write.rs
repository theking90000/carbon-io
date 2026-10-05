//! Write lifecycle, FIFO replacement, replay, buffering, and contract regressions.
mod support;
use carbon_io::{
    AsyncFilesWrite, ContractError as C, FrameBudget, WriteError as E, WriteScheduler,
};
use std::{
    pin::Pin,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn write_assigns_frames_strictly_by_capacity_and_handles_partial_final_file() {
    for retry in [0, 2] {
        for capacity in [4, 8, 32] {
            let budget = FrameBudget::new(capacity);
            let mut cfg = config(2, 100, 3, retry);
            let mut allocator = Allocator::default();
            let drops = allocator.drops.clone();
            let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
            for _ in 0..3 {
                writer.push_file(Write::new(4)).unwrap();
            }
            for value in 0..10 {
                write_value(&mut writer, value);
            }
            assert_eq!(
                close_writer(&mut writer),
                [vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9]]
            );
            assert_eq!(drops.get(), 10);
            assert_eq!(budget.available_capacity(), capacity);
        }
    }
}

#[test]
fn write_opens_only_used_lazy_destinations() {
    for retry in [0, 2] {
        for count in [0, 3, 100, 101] {
            let budget = FrameBudget::new(1000);
            let mut cfg = config(100, 1000, 10, retry);
            let mut allocator = Allocator::default();
            let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
            let files: Vec<_> = (0..10).map(|_| Write::new(100)).collect();
            for file in &files {
                writer.push_file(file.clone()).unwrap();
            }
            assert!(
                files
                    .iter()
                    .all(|f| f.opens.get() == 0 && f.opening.polls() == 0)
            );
            for value in 0..count {
                write_value(&mut writer, value);
            }
            let results = close_writer(&mut writer);
            assert_eq!(
                results,
                (0..count)
                    .collect::<Vec<_>>()
                    .chunks(100)
                    .map(|c| c.to_vec())
                    .collect::<Vec<_>>()
            );
            for (index, file) in files.iter().enumerate() {
                assert_eq!(file.opens.get(), usize::from(index < count.div_ceil(100)));
                assert_eq!(
                    file.finalized.get(),
                    usize::from(index < count.div_ceil(100))
                );
            }
            assert_eq!(budget.available_capacity(), 1000);
        }
    }
}

#[test]
fn write_allows_concurrent_files_and_returns_results_in_fifo_order() {
    for retry in [0, 1] {
        let budget = FrameBudget::new(4);
        let mut cfg = config(4, 10, 2, retry);
        let mut allocator = Allocator::default();
        let drops = allocator.drops.clone();
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        let first = Write::new(2);
        first.finalizing.close();
        let second = Write::new(2);
        writer.push_file(first.clone()).unwrap();
        writer.push_file(second.clone()).unwrap();
        for value in 0..4 {
            write_value(&mut writer, value);
        }
        let (_, waker) = context_waker();
        let cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut writer).poll_close(&cx).is_pending());
        assert_eq!(second.finalized.get(), 1);
        assert!(writer.pop_file().unwrap().is_none());
        assert_eq!(writer.retained_frames(), if retry == 0 { 0 } else { 2 });
        assert_eq!(drops.get(), if retry == 0 { 4 } else { 2 });
        assert_eq!(budget.available_capacity(), if retry == 0 { 4 } else { 2 });
        first.finalizing.open();
        assert_eq!(close_writer(&mut writer), [vec![0, 1], vec![2, 3]]);
    }
}

#[test]
fn replay_retains_frames_until_close_and_streaming_releases_each_success() {
    for retry in [0, 1] {
        let budget = FrameBudget::new(4);
        let mut cfg = config(4, 4, 1, retry);
        let mut allocator = Allocator::default();
        let drops = allocator.drops.clone();
        let file = Write::new(4);
        file.finalizing.close();
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        writer.push_file(file.clone()).unwrap();
        for value in 0..4 {
            write_value(&mut writer, value);
        }
        let (_, waker) = context_waker();
        assert!(
            Pin::new(&mut writer)
                .poll_close(&Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(drops.get(), if retry == 0 { 4 } else { 0 });
        assert_eq!(writer.retained_frames(), if retry == 0 { 0 } else { 4 });
        file.finalizing.open();
        assert_eq!(close_writer(&mut writer), [vec![0, 1, 2, 3]]);
        assert_eq!(drops.get(), 4);
        assert_eq!(budget.available_capacity(), 4);
    }
}

#[test]
fn zero_retry_buffers_only_to_capacity_and_resumes_after_backend_wake() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 100, 1, 0);
    let mut allocator = Allocator::default();
    let drops = allocator.drops.clone();
    let file = Write::new(100);
    file.writing.close();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file.clone()).unwrap();
    write_value(&mut writer, 0);
    write_value(&mut writer, 1);
    let (wakes, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut writer).poll_write(&cx, &2).is_pending());
    assert_eq!(writer.retained_frames(), 2);
    assert_eq!(drops.get(), 0);
    let before = wakes.0.load(Ordering::Relaxed);
    file.writing.open();
    assert!(wakes.0.load(Ordering::Relaxed) > before);
    for value in 2..100 {
        write_value(&mut writer, value);
    }
    assert_eq!(close_writer(&mut writer), [(0..100).collect::<Vec<_>>()]);
    assert_eq!(drops.get(), 100);
    assert_eq!(budget.available_capacity(), 2);
}

#[test]
fn write_replays_from_first_frame_after_open_write_or_close_error() {
    for failure in ["open", "write", "finalize"] {
        let budget = FrameBudget::new(4);
        let mut cfg = config(2, 10, 1, 1);
        let mut allocator = Allocator::default();
        let drops = allocator.drops.clone();
        let file = Write::new(4);
        match failure {
            "open" => file.open_failures.set(1),
            "write" => file.write_failures.set(1),
            _ => file.finalize_failures.set(1),
        }
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        writer.push_file(file.clone()).unwrap();
        for value in 0..4 {
            write_value(&mut writer, value);
        }
        assert_eq!(close_writer(&mut writer), [vec![0, 1, 2, 3]]);
        assert_eq!(file.opens.get(), 2);
        let attempts = file.attempts.borrow();
        match failure {
            "open" => assert_eq!(*attempts, [vec![0, 1, 2, 3]]),
            "write" => assert_eq!(*attempts, [vec![0], vec![0, 1, 2, 3]]),
            _ => assert_eq!(*attempts, [vec![0, 1, 2, 3], vec![0, 1, 2, 3]]),
        }
        assert_eq!(drops.get(), 4);
        assert_eq!(budget.available_capacity(), 4);
    }
}

#[test]
fn write_fails_after_max_retries_and_cancels_work() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 10, 1, 1);
    let mut allocator = Allocator::default();
    let drops = allocator.drops.clone();
    let file = Write::new(2);
    file.open_failures.set(1);
    file.finalize_failures.set(1);
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file).unwrap();
    for value in 0..2 {
        write_value(&mut writer, value);
    }
    let (_, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    assert_eq!(
        Pin::new(&mut writer).poll_close(&cx),
        Poll::Ready(Err(E::Backend("finalize")))
    );
    assert_eq!(drops.get(), 2);
    assert_eq!(budget.available_capacity(), 2);
    assert!(!writer.accept_file());
    assert_eq!(
        Pin::new(&mut writer).poll_write(&cx, &2),
        Poll::Ready(Err(E::Contract(C::InputClosed)))
    );
    assert_eq!(
        Pin::new(&mut writer).poll_close(&cx),
        Poll::Ready(Err(E::Contract(C::InputClosed)))
    );
}

#[test]
fn rejected_operations_preserve_accepted_work() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 10, 1, 1);
    let mut allocator = Allocator::default();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    for (capacity, error) in [
        (0, C::ZeroFrameCapacity),
        (5, C::FrameCapacityExceedsBudget),
    ] {
        assert_eq!(
            writer.push_file(Write::new(capacity)),
            Err(E::Contract(error))
        );
        assert!(writer.accept_file());
    }
    writer.push_file(Write::new(4)).unwrap();
    assert_eq!(
        writer.push_file(Write::new(4)),
        Err(E::Contract(C::AdmissionNotReady))
    );
    assert_eq!(
        writer.pop_swap(Write::new(4)).err(),
        Some(E::Contract(C::AdmissionNotReady))
    );
    assert_eq!(writer.pop_file().err(), Some(E::Contract(C::InputClosed)));
    assert_eq!(
        writer.set_window(config(0, 10, 1, 0).window),
        Err(C::InvalidWindow)
    );
    for value in 0..4 {
        write_value(&mut writer, value);
    }
    let (_, waker) = context_waker();
    let Poll::Ready(Ok(status)) =
        Pin::new(&mut writer).poll_write(&Context::from_waker(&waker), &4)
    else {
        panic!("missing status")
    };
    assert_eq!(status.output, None);
    assert_eq!(status.completed_files, 1);
    assert_eq!(
        writer.pop_swap(Write::new(0)).err(),
        Some(E::Contract(C::ZeroFrameCapacity))
    );
    let (previous, output) = writer.pop_swap(Write::new(2)).unwrap();
    assert_eq!(previous.capacity, 4);
    assert_eq!(output, [0, 1, 2, 3]);
    write_value(&mut writer, 4);
    assert_eq!(close_writer(&mut writer), [vec![4]]);
    assert_eq!(
        writer.push_file(Write::new(1)),
        Err(E::Contract(C::InputClosed))
    );
    assert_eq!(
        writer.pop_swap(Write::new(1)).err(),
        Some(E::Contract(C::InputClosed))
    );
}

#[test]
fn completed_heads_are_replaced_at_the_tail_before_more_input() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 8, 2, 1);
    let mut allocator = Allocator::default();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(Write::new(1)).unwrap();
    writer.push_file(Write::new(2)).unwrap();
    let (_, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    let mut outputs = Vec::new();
    for value in 0..12 {
        loop {
            let Poll::Ready(Ok(status)) = Pin::new(&mut writer).poll_write(&cx, &value) else {
                panic!("stalled")
            };
            for _ in 0..status.completed_files {
                outputs.push(writer.pop_swap(Write::new(2)).unwrap().1);
            }
            if status.output.is_some() {
                break;
            }
        }
    }
    outputs.extend(close_writer(&mut writer));
    assert_eq!(outputs.concat(), (0..12).collect::<Vec<_>>());
    assert_eq!(outputs[0], [0]);
    assert_eq!(outputs[1], [1, 2]);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn missing_descriptors_are_reported_before_input_is_consumed() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let mut allocator = Allocator::default();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    let (_, waker) = context_waker();
    let Poll::Ready(Ok(status)) =
        Pin::new(&mut writer).poll_write(&Context::from_waker(&waker), &7)
    else {
        panic!("missing status")
    };
    assert_eq!(status.output, None);
    assert_eq!(status.missing_files, 1);
    assert_eq!(writer.granted_frames(), 0);
    writer.push_file(Write::new(1)).unwrap();
    write_value(&mut writer, 7);
    assert_eq!(close_writer(&mut writer), [vec![7]]);
}

#[test]
fn dropping_writer_cancels_pending_io_and_returns_all_capacity() {
    for retry in [0, 1] {
        let budget = FrameBudget::new(4);
        let mut cfg = config(4, 4, 1, retry);
        let mut allocator = Allocator::default();
        let drops = allocator.drops.clone();
        let file = Write::new(4);
        file.writing.close();
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
        writer.push_file(file.clone()).unwrap();
        for value in 0..4 {
            write_value(&mut writer, value);
        }
        let (_, waker) = context_waker();
        assert!(
            Pin::new(&mut writer)
                .poll_write(&Context::from_waker(&waker), &4)
                .is_pending()
        );
        assert_eq!(drops.get(), 0);
        drop(writer);
        assert_eq!(drops.get(), 4);
        assert_eq!(file.drops.get(), 1);
        assert_eq!(budget.available_capacity(), 4);
    }
}

#[test]
fn shrinking_streaming_window_keeps_pending_frames_and_updates_callers_config() {
    let budget = FrameBudget::new(16);
    let mut cfg = config(8, 100, 1, 0);
    let mut allocator = Allocator::default();
    let file = Write::new(100);
    file.writing.close();
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(file.clone()).unwrap();
    for value in 0..8 {
        write_value(&mut writer, value);
    }
    let window = config(2, 100, 1, 0).window;
    writer.set_window(window).unwrap();
    assert_eq!(writer.retained_frames(), 8);
    file.writing.open();
    write_value(&mut writer, 8);
    assert!(writer.granted_frames() <= 2);
    assert_eq!(close_writer(&mut writer), [(0..9).collect::<Vec<_>>()]);
    drop(writer);
    assert_eq!(cfg.window, window);
}

#[test]
fn future_commit_survives_an_earlier_fatal_failure() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 4, 2, 0);
    let mut allocator = Allocator::default();
    let first = Write::new(2);
    first.finalizing.close();
    first.finalize_failures.set(1);
    let second = Write::new(2);
    let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut cfg).unwrap();
    writer.push_file(first.clone()).unwrap();
    writer.push_file(second.clone()).unwrap();
    for value in 0..4 {
        write_value(&mut writer, value);
    }
    let (_, waker) = context_waker();
    let cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut writer).poll_close(&cx).is_pending());
    assert_eq!(second.finalized.get(), 1);
    first.finalizing.open();
    assert_eq!(
        Pin::new(&mut writer).poll_close(&cx),
        Poll::Ready(Err(E::Backend("finalize")))
    );
    assert_eq!(second.finalized.get(), 1);
    assert_eq!(budget.available_capacity(), 4);
}

#[test]
fn initial_configuration_errors_are_returned_without_allocation() {
    for (capacity, target, error) in [(0, 1, C::ZeroBudget), (1, 0, C::InvalidWindow)] {
        let budget = FrameBudget::new(capacity);
        let mut cfg = config(target, 1, 1, 0);
        let mut allocator = Allocator::default();
        assert_eq!(
            Writer::new(&mut allocator, &budget, &mut cfg).err(),
            Some(error)
        );
        assert_eq!(budget.available_capacity(), capacity);
    }
}
