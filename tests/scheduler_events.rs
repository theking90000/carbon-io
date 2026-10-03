//! Combined scheduler events, fairness, cancellation, and bounded progress.
mod support;

use carbon_io::{
    ContractError, FrameBudget, Interest, ReadEvent, ReadScheduler, SchedulerError, WriteEvent,
    WriteScheduler,
};
use futures::executor::block_on;
use std::{
    cell::Cell,
    collections::VecDeque,
    future::Future,
    rc::Rc,
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
fn ignored_write_admissions_preserve_grants_and_wake_once_for_a_batch() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 8, 2, 0);
    let mut writer = WriteScheduler::new(&budget, &mut cfg);
    let drops = Rc::new(Cell::new(0));
    assert!(writer.accept_file());
    writer.enqueue_file(Write::new(2)).unwrap();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        writer.poll_with_interest(&mut cx, Interest::FRAMES),
        Poll::Ready(Ok(Some(WriteEvent::FrameReady)))
    );
    let capacity = writer.granted_frames();
    let before = wakes.0.load(Ordering::Relaxed);
    for _ in 0..8 {
        assert!(
            writer
                .poll_with_interest(&mut cx, Interest::RESULTS)
                .is_pending()
        );
    }
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    assert_eq!(writer.granted_frames(), capacity);
    for value in [10, 20] {
        assert!(writer.accept_frame());
        writer
            .enqueue_frame(Frame {
                value,
                drops: drops.clone(),
            })
            .unwrap();
    }
    writer.close_frames().unwrap();
    writer.close_files().unwrap();
    assert_eq!(wakes.0.load(Ordering::Relaxed), before + 1);
    assert_eq!(collect(writer), [Ok(vec![10, 20])]);
    assert_eq!(drops.get(), 2);
    assert_eq!(budget.available_capacity(), 2);
}

#[test]
fn interests_do_not_create_unused_frame_grants_and_keep_rotating() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 8, 3, 0);
    let mut writer = WriteScheduler::new(&budget, &mut cfg);
    assert!(writer.accept_file());
    writer.enqueue_file(Write::new(1)).unwrap();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(
        writer
            .poll_with_interest(&mut cx, Interest::RESULTS)
            .is_pending()
    );
    assert_eq!(writer.granted_frames(), 0);
    assert_eq!(budget.available_capacity(), 2);
    assert!(writer.accept_frame());
    writer
        .enqueue_frame(Frame {
            value: 7,
            drops: Rc::new(Cell::new(0)),
        })
        .unwrap();

    let interest = Interest::FILES | Interest::RESULTS;
    assert_eq!(
        writer.poll_with_interest(&mut cx, interest),
        Poll::Ready(Ok(Some(WriteEvent::FileReady)))
    );
    assert_eq!(
        writer.poll_with_interest(&mut cx, interest),
        Poll::Ready(Ok(Some(WriteEvent::File(vec![7]))))
    );
    writer.close_frames().unwrap();
    writer.close_files().unwrap();
    assert_eq!(
        writer.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Ok(None))
    );
}

#[test]
fn write_admission_misuse_returns_errors_instead_of_panicking() {
    for scenario in 0..6 {
        let budget = FrameBudget::new(2);
        let mut cfg = config(2, 4, 1, 0);
        let mut writer = WriteScheduler::new(&budget, &mut cfg);
        let file = Write::new(2);
        let drops = Rc::new(Cell::new(0));
        let frame = || Frame {
            value: 1,
            drops: drops.clone(),
        };
        let (_, waker) = context_waker();
        let mut cx = Context::from_waker(&waker);
        let expected = if scenario < 2 {
            ContractError::AdmissionNotReady
        } else {
            ContractError::InputClosed
        };
        let result = match scenario {
            0 => writer.enqueue_file(file.clone()),
            1 => writer.enqueue_frame(frame()),
            2 => {
                writer.close_files().unwrap();
                writer.enqueue_file(file.clone())
            }
            3 => {
                writer.close_files().unwrap();
                match writer.poll_file_ready(&mut cx) {
                    Poll::Ready(result) => result,
                    Poll::Pending => panic!("closed input must return an error"),
                }
            }
            4 => {
                assert!(writer.accept_file());
                writer.enqueue_file(file.clone()).unwrap();
                assert!(writer.accept_frame());
                writer.close_frames().unwrap();
                writer.enqueue_frame(frame())
            }
            _ => {
                writer.close_frames().unwrap();
                match writer.poll_frame_ready(&mut cx) {
                    Poll::Ready(result) => result,
                    Poll::Pending => panic!("closed input must return an error"),
                }
            }
        };
        assert_eq!(result, Err(SchedulerError::Contract(expected)));
        assert_eq!(file.opens.get(), 0);
        assert_eq!(budget.available_capacity(), 2);
        assert_eq!(writer.poll(&mut cx), Poll::Ready(Ok(None)));
        assert_eq!(
            writer.enqueue_file(file),
            Err(SchedulerError::Contract(ContractError::InputClosed))
        );
        assert_eq!(
            writer.enqueue_frame(frame()),
            Err(SchedulerError::Contract(ContractError::InputClosed))
        );
        assert_eq!(
            writer.poll_file_ready(&mut cx),
            Poll::Ready(Err(SchedulerError::Contract(ContractError::InputClosed)))
        );
        assert_eq!(
            writer.poll_frame_ready(&mut cx),
            Poll::Ready(Err(SchedulerError::Contract(ContractError::InputClosed)))
        );
    }
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
fn write_notifications_fill_the_window_and_budget_without_polling_io() {
    for (ahead, active) in [(6, 10), (20, 2)] {
        let budget = FrameBudget::new(2);
        let mut cfg = config(2, ahead, active, 0);
        let mut writer = WriteScheduler::new(&budget, &mut cfg);
        let descriptors = [Write::new(4), Write::new(4), Write::new(4)];
        let mut files = VecDeque::from(descriptors.clone());
        let drops = Rc::new(Cell::new(0));
        let mut frames: VecDeque<_> = (0..5)
            .map(|value| Frame {
                value,
                drops: drops.clone(),
            })
            .collect();

        assert_eq!(
            block_on(writer.next_event()),
            Ok(Some(WriteEvent::FileReady))
        );
        while !files.is_empty() && writer.accept_file() {
            assert!(writer.accept_file());
            writer.enqueue_file(files.pop_front().unwrap()).unwrap();
        }
        assert_eq!(files.len(), 1);
        assert!(!writer.accept_file());
        writer.close_files().unwrap();
        assert!(!writer.accept_file());
        assert_eq!(
            block_on(writer.next_event()),
            Ok(Some(WriteEvent::FrameReady))
        );

        while !frames.is_empty() && writer.accept_frame() {
            assert!(writer.accept_frame());
            writer.enqueue_frame(frames.pop_front().unwrap()).unwrap();
        }
        assert_eq!(frames.len(), 3);
        assert!(!writer.accept_frame());
        assert_eq!(drops.get(), 0);
        assert_eq!(budget.available_capacity(), 0);
        for file in &descriptors {
            assert_eq!(file.opening.polls(), 0);
            assert_eq!(file.writing.polls(), 0);
            assert_eq!(file.finalizing.polls(), 0);
        }

        let mut results = Vec::new();
        while let Some(event) = block_on(writer.next_event()).unwrap() {
            match event {
                WriteEvent::FrameReady => {
                    while !frames.is_empty() && writer.accept_frame() {
                        writer.enqueue_frame(frames.pop_front().unwrap()).unwrap();
                    }
                    if frames.is_empty() {
                        writer.close_frames().unwrap();
                        assert!(!writer.accept_frame());
                    }
                }
                WriteEvent::File(result) => results.push(result),
                WriteEvent::FileReady => panic!("closed file input emitted readiness"),
            }
        }
        assert_eq!(results, [vec![0, 1, 2, 3], vec![4]]);
        assert_eq!(drops.get(), 5);
        assert_eq!(budget.available_capacity(), 2);
        assert_eq!(descriptors[2].opens.get(), 0);
    }
}

#[test]
fn synchronous_admission_checks_preserve_errors_for_the_next_poll() {
    let budget = FrameBudget::new(0);
    let mut read_cfg = config(1, 1, 1, 0);
    let mut write_cfg = read_cfg;
    let mut reader = ReadScheduler::<usize, Read>::new(&budget, &mut read_cfg);
    let mut writer = WriteScheduler::<Frame, Write>::new(&budget, &mut write_cfg);

    assert!(!reader.accept_file());
    assert!(!writer.accept_file());
    assert!(!writer.accept_frame());
    assert_eq!(
        block_on(reader.next_event()),
        Err(SchedulerError::Contract(ContractError::ZeroBudget))
    );
    assert_eq!(
        block_on(writer.next_event()),
        Err(SchedulerError::Contract(ContractError::ZeroBudget))
    );
    assert!(!reader.accept_file());
    assert!(!writer.accept_file());
    assert!(!writer.accept_frame());
    assert_eq!(block_on(reader.next_event()), Ok(None));
    assert_eq!(block_on(writer.next_event()), Ok(None));
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
fn write_events_rotate_all_ready_kinds_and_preserve_frame_grants() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 10, 3, 0);
    let mut writer = WriteScheduler::new(&budget, &mut cfg);
    let drops = Rc::new(Cell::new(0));
    for _ in 0..2 {
        block_on(writer.file_ready()).unwrap();
        writer.enqueue_file(Write::new(1)).unwrap();
    }
    block_on(writer.frame_ready()).unwrap();
    writer
        .enqueue_frame(Frame {
            value: 0,
            drops: drops.clone(),
        })
        .unwrap();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);

    assert_eq!(
        writer.poll(&mut cx),
        Poll::Ready(Ok(Some(WriteEvent::FileReady)))
    );
    assert_eq!(
        writer.poll(&mut cx),
        Poll::Ready(Ok(Some(WriteEvent::FrameReady)))
    );
    assert_eq!(
        writer.poll(&mut cx),
        Poll::Ready(Ok(Some(WriteEvent::File(vec![0]))))
    );
    writer
        .enqueue_frame(Frame {
            value: 1,
            drops: drops.clone(),
        })
        .unwrap();
    writer.close_frames().unwrap();
    writer.close_files().unwrap();
    assert_eq!(
        block_on(writer.next_event()),
        Ok(Some(WriteEvent::File(vec![1])))
    );
    assert_eq!(block_on(writer.next_event()), Ok(None));
    assert_eq!(drops.get(), 2);
    assert_eq!(budget.available_capacity(), 2);
}

#[test]
fn write_events_keep_results_available_before_frame_eof_is_declared() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let mut writer = WriteScheduler::new(&budget, &mut cfg);
    block_on(writer.file_ready()).unwrap();
    writer.enqueue_file(Write::new(1)).unwrap();
    writer.close_files().unwrap();
    block_on(writer.frame_ready()).unwrap();
    writer
        .enqueue_frame(Frame {
            value: 3,
            drops: Rc::new(Cell::new(0)),
        })
        .unwrap();
    assert_eq!(
        block_on(writer.next_event()),
        Ok(Some(WriteEvent::File(vec![3])))
    );
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(writer.poll(&mut cx).is_pending());
    writer.close_frames().unwrap();
    assert_eq!(block_on(writer.next_event()), Ok(None));
}

#[test]
fn cancelling_frame_ready_preserves_budget_wait_and_granted_admission() {
    let budget = FrameBudget::new(1);
    let mut held = budget.permit();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(held.poll_grow(&mut cx, 1, 1).is_ready());
    let mut cfg = config(1, 1, 1, 0);
    let mut writer = WriteScheduler::new(&budget, &mut cfg);
    block_on(writer.file_ready()).unwrap();
    writer.enqueue_file(Write::new(1)).unwrap();
    let mut waiting = Box::pin(writer.frame_ready());
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(waiting);
    let before = wakes.0.load(Ordering::Relaxed);
    drop(held);
    assert!(wakes.0.load(Ordering::Relaxed) > before);

    let mut waiting = Box::pin(writer.frame_ready());
    assert_eq!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    drop(waiting);
    writer.poll_progress(&mut cx);
    writer.set_window(config(1, 1, 1, 0).window).unwrap();
    writer
        .enqueue_frame(Frame {
            value: 7,
            drops: Rc::new(Cell::new(0)),
        })
        .unwrap();
    writer.close_frames().unwrap();
    writer.close_files().unwrap();
    assert_eq!(
        block_on(writer.next_event()),
        Ok(Some(WriteEvent::File(vec![7])))
    );
    assert_eq!(block_on(writer.next_event()), Ok(None));
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
    let mut write_cfg = read_cfg;
    let mut reader = ReadScheduler::<usize, Read>::new(&budget, &mut read_cfg);
    let mut writer = WriteScheduler::<Frame, Write>::new(&budget, &mut write_cfg);
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        reader.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Err(SchedulerError::Contract(ContractError::ZeroBudget)))
    );
    assert_eq!(
        writer.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Err(SchedulerError::Contract(ContractError::ZeroBudget)))
    );
    assert_eq!(
        reader.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Ok(None))
    );
    assert_eq!(
        writer.poll_with_interest(&mut cx, Interest::NONE),
        Poll::Ready(Ok(None))
    );
}
