//! Scheduler contract regression tests.
mod support;
use carbon_io::{ContractError as C, FrameBudget, SchedulerError as E};
use futures::stream;
use std::task::Poll;
use support::*;

#[test]
fn write_assigns_frames_strictly_by_capacity() {
    for retry in [0, 2] {
        for budget in [4, 8, 32] {
            let (input, _) = frames(10);
            let mut scheduler_input_1 = input;
            let mut scheduler_files_1 =
                std::pin::pin!(stream::iter([Write::new(4), Write::new(4), Write::new(4)]));
            let scheduler_budget_1 = FrameBudget::new(budget);
            let mut scheduler_config_1 = config(2, 100, 3, retry);
            let s = WriteDriver::new(
                &mut scheduler_input_1,
                &mut scheduler_files_1,
                &scheduler_budget_1,
                &mut scheduler_config_1,
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
    let mut scheduler_input_2 = input.chain(stream::pending());
    let mut scheduler_files_2 = stream::iter([f.clone()]);
    let scheduler_budget_2 = FrameBudget::new(4);
    let mut scheduler_config_2 = config(4, 10, 2, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_2,
        &mut scheduler_files_2,
        &scheduler_budget_2,
        &mut scheduler_config_2,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(*f.attempts.borrow(), [vec![0]]);
    assert_eq!(f.finalized.get(), 0);
}
#[test]
fn write_does_not_open_destinations_without_frames() {
    for retry in [0, 2] {
        let files: Vec<_> = (0..10).map(|_| Write::new(100)).collect();
        let mut scheduler_input_3 = stream::pending::<Frame>();
        let mut scheduler_files_3 = stream::iter(files.clone());
        let scheduler_budget_3 = FrameBudget::new(1000);
        let mut scheduler_config_3 = config(100, 1000, 10, retry);
        let mut s = WriteDriver::new(
            &mut scheduler_input_3,
            &mut scheduler_files_3,
            &scheduler_budget_3,
            &mut scheduler_config_3,
        );
        for _ in 0..3 {
            assert!(poll(&mut s).is_pending());
        }
        assert!(files.iter().all(|f| f.opens.get() == 0));
        assert!(files.iter().all(|f| f.opening.polls() == 0));
    }
}
#[test]
fn write_opens_only_used_lazy_destinations() {
    for retry in [0, 2] {
        for n in [0, 3, 100, 101] {
            let mut files = Vec::new();
            let destinations = stream::iter((0..).map(|_| {
                let file = Write::new(100);
                files.push(file.clone());
                file
            }));
            let (input, drops) = frames(n);
            let budget = FrameBudget::new(1000);
            let mut scheduler_input_4 = input;
            let mut scheduler_files_4 = destinations;
            let mut scheduler_config_4 = config(100, 1000, 10, retry);
            let results = collect(WriteDriver::new(
                &mut scheduler_input_4,
                &mut scheduler_files_4,
                &budget,
                &mut scheduler_config_4,
            ));
            let values: Vec<_> = (0..n).collect();
            assert_eq!(
                results,
                values
                    .chunks(100)
                    .map(|c| Ok(c.to_vec()))
                    .collect::<Vec<_>>()
            );
            let used = n.div_ceil(100);
            for (i, file) in files.iter().enumerate() {
                assert_eq!(file.opens.get(), usize::from(i < used));
                assert_eq!(file.finalized.get(), usize::from(i < used));
            }
            assert_eq!(drops.get(), n);
            assert_eq!(budget.available_capacity(), 1000);
        }
    }
}
#[test]
fn write_starts_next_writer_when_previous_file_capacity_is_reached() {
    let a = Write::new(4);
    a.finalizing.close();
    let b = Write::new(4);
    let (input, _) = frames(8);
    let mut scheduler_input_5 = input;
    let mut scheduler_files_5 = stream::iter([a.clone(), b.clone()]);
    let scheduler_budget_5 = FrameBudget::new(8);
    let mut scheduler_config_5 = config(8, 100, 2, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_5,
        &mut scheduler_files_5,
        &scheduler_budget_5,
        &mut scheduler_config_5,
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
    let mut scheduler_input_6 = input;
    let mut scheduler_files_6 = stream::iter([a.clone(), b.clone()]);
    let scheduler_budget_6 = FrameBudget::new(4);
    let mut scheduler_config_6 = config(4, 10, 2, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_6,
        &mut scheduler_files_6,
        &scheduler_budget_6,
        &mut scheduler_config_6,
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
    let mut scheduler_input_7 = input;
    let mut scheduler_files_7 = stream::iter([f.clone()]);
    let scheduler_budget_7 = FrameBudget::new(4);
    let mut scheduler_config_7 = config(4, 10, 1, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_7,
        &mut scheduler_files_7,
        &scheduler_budget_7,
        &mut scheduler_config_7,
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
    let mut scheduler_input_8 = input;
    let mut scheduler_files_8 = stream::iter([f.clone()]);
    let scheduler_budget_8 = FrameBudget::new(2);
    let mut scheduler_config_8 = config(2, 100, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_8,
        &mut scheduler_files_8,
        &scheduler_budget_8,
        &mut scheduler_config_8,
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
    let mut scheduler_input_9 = input;
    let mut scheduler_files_9 = stream::iter([f.clone()]);
    let scheduler_budget_9 = FrameBudget::new(2);
    let mut scheduler_config_9 = config(2, 100, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_9,
        &mut scheduler_files_9,
        &scheduler_budget_9,
        &mut scheduler_config_9,
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
    let mut scheduler_input_10 = input;
    let mut scheduler_files_10 = stream::iter([f.clone()]);
    let scheduler_budget_10 = FrameBudget::new(4);
    let mut scheduler_config_10 = config(2, 10, 1, 1);
    let s = WriteDriver::new(
        &mut scheduler_input_10,
        &mut scheduler_files_10,
        &scheduler_budget_10,
        &mut scheduler_config_10,
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
    let mut scheduler_input_11 = input;
    let mut scheduler_files_11 = stream::iter([f.clone()]);
    let scheduler_budget_11 = FrameBudget::new(4);
    let mut scheduler_config_11 = config(2, 10, 1, 1);
    let s = WriteDriver::new(
        &mut scheduler_input_11,
        &mut scheduler_files_11,
        &scheduler_budget_11,
        &mut scheduler_config_11,
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
    let mut scheduler_input_12 = input;
    let mut scheduler_files_12 = stream::iter([f]);
    let scheduler_budget_12 = FrameBudget::new(2);
    let mut scheduler_config_12 = config(2, 10, 1, 1);
    let s = WriteDriver::new(
        &mut scheduler_input_12,
        &mut scheduler_files_12,
        &scheduler_budget_12,
        &mut scheduler_config_12,
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
    let mut scheduler_input_13 = input;
    let mut scheduler_files_13 = stream::iter([a, b]);
    let mut scheduler_config_13 = config(8, 100, 2, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_13,
        &mut scheduler_files_13,
        &budget,
        &mut scheduler_config_13,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(drops.get(), 4);
    assert_eq!(s.retained_frames(), 4);
    assert_eq!(budget.available_capacity(), 4);
}
#[test]
fn write_handles_partial_final_file() {
    let (input, _) = frames(2);
    let mut scheduler_input_14 = input;
    let mut scheduler_files_14 = stream::iter([Write::new(4)]);
    let scheduler_budget_14 = FrameBudget::new(4);
    let mut scheduler_config_14 = config(1, 10, 1, 1);
    assert_eq!(
        collect(WriteDriver::new(
            &mut scheduler_input_14,
            &mut scheduler_files_14,
            &scheduler_budget_14,
            &mut scheduler_config_14,
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
        let mut scheduler_input_15 = input;
        let mut scheduler_files_15 = stream::iter([a.clone(), b.clone()]);
        let scheduler_budget_15 = FrameBudget::new(8);
        let mut scheduler_config_15 = config(8, 100, 2, 1);
        let results = collect(WriteDriver::new(
            &mut scheduler_input_15,
            &mut scheduler_files_15,
            &scheduler_budget_15,
            &mut scheduler_config_15,
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
        let mut scheduler_input_16 = input;
        let mut scheduler_files_16 = stream::iter(files);
        let scheduler_budget_16 = FrameBudget::new(4);
        let mut scheduler_config_16 = config(4, 10, 2, 1);
        let result = collect(WriteDriver::new(
            &mut scheduler_input_16,
            &mut scheduler_files_16,
            &scheduler_budget_16,
            &mut scheduler_config_16,
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
    let mut scheduler_input_17 = input;
    let mut scheduler_files_17 = stream::iter([Write::new(5)]);
    let scheduler_budget_17 = FrameBudget::new(4);
    let mut scheduler_config_17 = config(4, 10, 2, 1);
    assert_eq!(
        collect(WriteDriver::new(
            &mut scheduler_input_17,
            &mut scheduler_files_17,
            &scheduler_budget_17,
            &mut scheduler_config_17,
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
    let mut scheduler_input_18 = input;
    let mut scheduler_files_18 = stream::iter([f]);
    let mut scheduler_config_18 = config(4, 10, 2, 1);
    let mut s = WriteDriver::new(
        &mut scheduler_input_18,
        &mut scheduler_files_18,
        &b,
        &mut scheduler_config_18,
    );
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
    let mut scheduler_input_19 = std::pin::pin!(input);
    let mut scheduler_files_19 = std::pin::pin!(files);
    let scheduler_budget_19 = FrameBudget::new(4);
    let mut scheduler_config_19 = config(4, 10, 1, 1);
    assert_eq!(
        collect(WriteDriver::new(
            &mut scheduler_input_19,
            &mut scheduler_files_19,
            &scheduler_budget_19,
            &mut scheduler_config_19,
        )),
        [Ok(vec![0, 1, 2, 3])]
    );
}
#[test]
fn write_rejects_zero_capacity() {
    let (input, _) = frames(1);
    let mut scheduler_input_20 = input;
    let mut scheduler_files_20 = stream::iter([Write::new(0)]);
    let scheduler_budget_20 = FrameBudget::new(4);
    let mut scheduler_config_20 = config(4, 10, 1, 0);
    assert_eq!(
        collect(WriteDriver::new(
            &mut scheduler_input_20,
            &mut scheduler_files_20,
            &scheduler_budget_20,
            &mut scheduler_config_20,
        )),
        [Err(E::Contract(C::ZeroFrameCapacity))]
    );
}
#[test]
fn write_is_fused_after_failure() {
    let (input, _) = frames(1);
    let mut scheduler_input_21 = input;
    let mut scheduler_files_21 = stream::empty::<Write>();
    let scheduler_budget_21 = FrameBudget::new(4);
    let mut scheduler_config_21 = config(4, 10, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_21,
        &mut scheduler_files_21,
        &scheduler_budget_21,
        &mut scheduler_config_21,
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
    let mut scheduler_input_22 = input.chain(stream::pending());
    let mut scheduler_files_22 = stream::iter([file.clone()]);
    let scheduler_budget_22 = FrameBudget::new(16);
    let mut scheduler_config_22 = config(8, 100, 1, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_22,
        &mut scheduler_files_22,
        &scheduler_budget_22,
        &mut scheduler_config_22,
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
    let mut scheduler_input_23 = input;
    let mut scheduler_files_23 = stream::iter([first.clone(), second.clone()]);
    let mut scheduler_config_23 = config(4, 100, 2, 0);
    let mut s = WriteDriver::new(
        &mut scheduler_input_23,
        &mut scheduler_files_23,
        &budget,
        &mut scheduler_config_23,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(second.finalized.get(), 1);
    first.finalizing.open();
    assert_eq!(collect(s), [Err(E::Backend("finalize"))]);
    assert_eq!(second.finalized.get(), 1);
    assert_eq!(drops.get(), 4);
    assert_eq!(budget.available_capacity(), 4);
}
