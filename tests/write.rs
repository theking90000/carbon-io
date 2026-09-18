//! Scheduler contract regression tests.
mod support;
use futures::stream;
use io_scheduler::{ContractError as C, FrameBudget, SchedulerError as E, WriteScheduler};
use std::task::Poll;
use support::*;

#[test]
fn write_assigns_frames_strictly_by_capacity() {
    for retry in [0, 2] {
        for budget in [4, 8, 32] {
            let (input, _) = frames(10);
            let s = WriteScheduler::new(
                input,
                stream::iter([Write::new(4), Write::new(4), Write::new(4)]),
                FrameBudget::new(budget),
                config(2, 100, 3, retry),
            );
            assert_eq!(
                collect(s),
                [Ok(vec![0, 1, 2, 3]), Ok(vec![4, 5, 6, 7]), Ok(vec![8, 9])]
            );
        }
    }
}
#[test]
fn write_starts_writer_on_first_frame() {
    use futures::StreamExt;
    let (input, _) = frames(1);
    let f = Write::new(4);
    let mut s = WriteScheduler::new(
        input.chain(stream::pending()),
        stream::iter([f.clone()]),
        FrameBudget::new(4),
        config(4, 10, 2, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(*f.attempts.borrow(), [vec![0]]);
    assert_eq!(f.finalized.get(), 0);
}
#[test]
fn write_opens_future_files_before_they_receive_frames() {
    let a = Write::new(4);
    let b = Write::new(4);
    let mut s = WriteScheduler::new(
        stream::pending::<Frame>(),
        stream::iter([a, b.clone()]),
        FrameBudget::new(4),
        config(4, 10, 2, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.opening.polls(), 1);
    assert_eq!(*b.attempts.borrow(), [Vec::<usize>::new()]);
}
#[test]
fn write_starts_next_writer_when_previous_file_capacity_is_reached() {
    let a = Write::new(4);
    a.finalizing.close();
    let b = Write::new(4);
    let (input, _) = frames(8);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([a.clone(), b.clone()]),
        FrameBudget::new(8),
        config(8, 100, 2, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.finalized.get(), 1);
    a.finalizing.open();
    assert_eq!(collect(s), [Ok(vec![0, 1, 2, 3]), Ok(vec![4, 5, 6, 7])]);
}
#[test]
fn write_allows_multiple_concurrent_writers() {
    let a = Write::new(2);
    a.writing.close();
    let b = Write::new(2);
    let (input, _) = frames(4);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([a.clone(), b.clone()]),
        FrameBudget::new(4),
        config(4, 10, 2, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.finalized.get(), 1);
    a.writing.open();
    assert_eq!(collect(s).len(), 2);
}
#[test]
fn write_retains_frames_after_successful_write() {
    let f = Write::new(4);
    f.finalizing.close();
    let (input, drops) = frames(4);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(4),
        config(4, 10, 1, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(drops.get(), 0);
    assert_eq!(s.retained_frames(), 4);
    f.finalizing.open();
    assert_eq!(collect(s), [Ok(vec![0, 1, 2, 3])]);
    assert_eq!(drops.get(), 4);
}
#[test]
fn write_releases_frames_only_after_finalize() {
    write_retains_frames_after_successful_write();
}
#[test]
fn zero_retry_drops_each_successful_write_before_finalize() {
    let f = Write::new(100);
    f.finalizing.close();
    let (input, drops) = frames(100);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(2),
        config(2, 100, 1, 0),
    );
    for _ in 0..10 {
        assert!(poll(&mut s).is_pending());
        if drops.get() == 100 {
            break;
        }
    }
    assert_eq!(drops.get(), 100);
    assert_eq!(s.retained_frames(), 0);
    assert_eq!(f.finalized.get(), 0);
    f.finalizing.open();
    assert_eq!(collect(s), [Ok((0..100).collect())]);
}
#[test]
fn zero_retry_preserves_pending_frames() {
    let f = Write::new(100);
    f.writing.close();
    let (input, drops) = frames(100);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(2),
        config(2, 100, 1, 0),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(s.retained_frames(), 2);
    assert_eq!(drops.get(), 0);
    f.writing.open();
    assert_eq!(collect(s), [Ok((0..100).collect())]);
    assert_eq!(drops.get(), 100);
}
#[test]
fn write_retries_from_first_frame_after_write_error() {
    let f = Write::new(4);
    f.write_failures.set(1);
    let (input, drops) = frames(4);
    let s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(4),
        config(2, 10, 1, 1),
    );
    assert_eq!(collect(s), [Ok(vec![0, 1, 2, 3])]);
    assert_eq!(*f.attempts.borrow(), [vec![0], vec![0, 1, 2, 3]]);
    assert_eq!(drops.get(), 4);
}
#[test]
fn write_retries_from_first_frame_after_finalize_error() {
    let f = Write::new(4);
    f.finalize_failures.set(1);
    let (input, _) = frames(4);
    let s = WriteScheduler::new(
        input,
        stream::iter([f.clone()]),
        FrameBudget::new(4),
        config(2, 10, 1, 1),
    );
    assert_eq!(collect(s), [Ok(vec![0, 1, 2, 3])]);
    assert_eq!(*f.attempts.borrow(), [vec![0, 1, 2, 3], vec![0, 1, 2, 3]]);
}
#[test]
fn write_fails_after_max_retries() {
    let f = Write::new(2);
    f.open_failures.set(1);
    f.finalize_failures.set(1);
    let (input, _) = frames(2);
    let s = WriteScheduler::new(
        input,
        stream::iter([f]),
        FrameBudget::new(2),
        config(2, 10, 1, 1),
    );
    assert_eq!(collect(s), [Err(E::Backend("finalize"))]);
}
#[test]
fn write_emits_finalize_results_in_file_order() {
    write_starts_next_writer_when_previous_file_capacity_is_reached();
}
#[test]
fn write_frees_committed_future_file_before_previous_result_is_ready() {
    let a = Write::new(4);
    a.finalizing.close();
    let b = Write::new(4);
    let (input, drops) = frames(8);
    let budget = FrameBudget::new(8);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([a, b]),
        budget.clone(),
        config(8, 100, 2, 1),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(drops.get(), 4);
    assert_eq!(s.retained_frames(), 4);
    assert_eq!(budget.available_capacity(), 4);
}
#[test]
fn write_handles_partial_final_file() {
    let (input, _) = frames(2);
    assert_eq!(
        collect(WriteScheduler::new(
            input,
            stream::iter([Write::new(4)]),
            FrameBudget::new(4),
            config(1, 10, 1, 1)
        )),
        [Ok(vec![0, 1])]
    );
}
#[test]
fn write_does_not_create_empty_final_file() {
    for n in [0, 4] {
        let a = Write::new(4);
        let b = Write::new(4);
        let (input, _) = frames(n);
        let results = collect(WriteScheduler::new(
            input,
            stream::iter([a.clone(), b.clone()]),
            FrameBudget::new(8),
            config(8, 100, 2, 1),
        ));
        assert_eq!(results.len(), usize::from(n > 0));
        assert_eq!(b.finalized.get(), 0);
        assert_eq!(a.finalized.get(), usize::from(n > 0));
    }
}
#[test]
fn write_fails_when_write_files_run_out() {
    for n in [0, 1, 5] {
        let (input, _) = frames(n);
        let files = if n == 5 { vec![Write::new(4)] } else { vec![] };
        let result = collect(WriteScheduler::new(
            input,
            stream::iter(files),
            FrameBudget::new(4),
            config(4, 10, 2, 1),
        ));
        if n == 0 {
            assert!(result.is_empty());
        } else {
            assert_eq!(result.last(), Some(&Err(E::Contract(C::MissingWriteFile))));
        }
    }
}
#[test]
fn write_rejects_file_larger_than_global_budget() {
    let (input, _) = frames(1);
    assert_eq!(
        collect(WriteScheduler::new(
            input,
            stream::iter([Write::new(5)]),
            FrameBudget::new(4),
            config(4, 10, 2, 1)
        )),
        [Err(E::Contract(C::FrameCapacityExceedsBudget))]
    );
}
#[test]
fn write_releases_budget_on_drop() {
    let f = Write::new(4);
    f.writing.close();
    let (input, drops) = frames(4);
    let b = FrameBudget::new(4);
    let mut s = WriteScheduler::new(input, stream::iter([f]), b.clone(), config(4, 10, 2, 1));
    assert!(poll(&mut s).is_pending());
    drop(s);
    assert_eq!(b.available_capacity(), 4);
    assert_eq!(drops.get(), 4);
}
#[test]
fn write_accepts_unpin_free_input() {
    let (input, _) = frames(4);
    let input = PinnedStream::new(input);
    let files = PinnedStream::new(stream::iter([Write::new(4)]));
    assert_eq!(
        collect(WriteScheduler::new(
            input,
            files,
            FrameBudget::new(4),
            config(4, 10, 1, 1)
        )),
        [Ok(vec![0, 1, 2, 3])]
    );
}
#[test]
fn write_rejects_zero_capacity() {
    let (input, _) = frames(1);
    assert_eq!(
        collect(WriteScheduler::new(
            input,
            stream::iter([Write::new(0)]),
            FrameBudget::new(4),
            config(4, 10, 1, 0)
        )),
        [Err(E::Contract(C::ZeroFrameCapacity))]
    );
}
#[test]
fn write_is_fused_after_failure() {
    let (input, _) = frames(1);
    let mut s = WriteScheduler::new(
        input,
        stream::empty::<Write>(),
        FrameBudget::new(4),
        config(4, 10, 1, 0),
    );
    assert_eq!(
        poll(&mut s),
        Poll::Ready(Some(Err(E::Contract(C::MissingWriteFile))))
    );
    assert_eq!(poll(&mut s), Poll::Ready(None));
}

#[test]
fn shrinking_zero_retry_window_takes_effect_while_file_is_partial() {
    use futures::StreamExt;
    let file = Write::new(100);
    file.writing.close();
    let (input, drops) = frames(8);
    let mut s = WriteScheduler::new(
        input.chain(stream::pending()),
        stream::iter([file.clone()]),
        FrameBudget::new(16),
        config(8, 100, 1, 0),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(s.retained_frames(), 8);
    s.set_window(config(2, 100, 1, 0).window).unwrap();
    file.writing.open();
    assert!(poll(&mut s).is_pending());
    assert_eq!(drops.get(), 8);
    assert_eq!(s.granted_frames(), 2);
}

#[test]
fn future_commit_survives_an_earlier_fatal_failure() {
    let first = Write::new(2);
    first.finalizing.close();
    first.finalize_failures.set(1);
    let second = Write::new(2);
    let (input, drops) = frames(4);
    let budget = FrameBudget::new(4);
    let mut s = WriteScheduler::new(
        input,
        stream::iter([first.clone(), second.clone()]),
        budget.clone(),
        config(4, 100, 2, 0),
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(second.finalized.get(), 1);
    first.finalizing.open();
    assert_eq!(collect(s), [Err(E::Backend("finalize"))]);
    assert_eq!(second.finalized.get(), 1);
    assert_eq!(drops.get(), 4);
    assert_eq!(budget.available_capacity(), 4);
}
