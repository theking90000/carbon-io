//! Scheduler contract regression tests.
mod support;
use carbon_io::{ContractError as C, FrameBudget, SchedulerError as E};
use futures::{StreamExt, stream};
use std::task::Poll;
use support::*;

#[test]
fn read_preserves_global_order() {
    for target in [1, 2, 7, 32, 300] {
        let files = (0..40).map(|i| Read::new(i * 3..i * 3 + 3));
        let mut scheduler_files_1 = stream::iter(files);
        let scheduler_budget_1 = FrameBudget::new(target);
        let mut scheduler_config_1 = config(target, 100, 10, 0);
        let result = collect(ReadDriver::new(
            &mut scheduler_files_1,
            &scheduler_budget_1,
            &mut scheduler_config_1,
        ));
        assert_eq!(result, (0..120).map(Ok).collect::<Vec<_>>());
    }
}
#[test]
fn read_opens_files_before_buffer_window_reaches_them() {
    let a = Read::new(0..6);
    a.reading.close();
    let b = Read::new(6..10);
    let mut scheduler_files_2 = stream::iter([a.clone(), b.clone()]);
    let scheduler_budget_2 = FrameBudget::new(4);
    let mut scheduler_config_2 = config(4, 100, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_2,
        &scheduler_budget_2,
        &mut scheduler_config_2,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.opens.get(), 1);
    assert_eq!(b.opening.polls(), 1);
    assert_eq!(b.reading.polls(), 0);
}
#[test]
fn read_does_not_poll_ready_reader_outside_buffer_window() {
    read_opens_files_before_buffer_window_reaches_them();
}
#[test]
fn read_window_is_contiguous_across_files() {
    let a = Read::new(0..6);
    a.reading.close();
    let b = Read::new(6..11);
    let c = Read::new(11..20);
    let mut scheduler_files_3 = stream::iter([a, b.clone(), c.clone()]);
    let scheduler_budget_3 = FrameBudget::new(8);
    let mut scheduler_config_3 = config(8, 100, 3, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_3,
        &scheduler_budget_3,
        &mut scheduler_config_3,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.reading.polls(), 2);
    assert_eq!(c.reading.polls(), 0);
}
#[test]
fn read_window_moves_when_front_frames_are_consumed() {
    let a = Read::new(0..6);
    let b = Read::new(6..11);
    let mut scheduler_files_4 = stream::iter([a, b.clone()]);
    let scheduler_budget_4 = FrameBudget::new(8);
    let mut scheduler_config_4 = config(8, 100, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_4,
        &scheduler_budget_4,
        &mut scheduler_config_4,
    );
    assert_eq!(poll(&mut s), Poll::Ready(Some(Ok(0))));
    assert_eq!(b.reading.polls(), 2);
    for value in 1..6 {
        assert_eq!(poll(&mut s), Poll::Ready(Some(Ok(value))));
    }
    assert_eq!(collect(s), (6..11).map(Ok).collect::<Vec<_>>());
}
#[test]
fn read_allows_multiple_readers_inside_window() {
    let a = Read::new(0..3);
    a.reading.close();
    let b = Read::new(3..6);
    let mut scheduler_files_5 = stream::iter([a.clone(), b.clone()]);
    let scheduler_budget_5 = FrameBudget::new(6);
    let mut scheduler_config_5 = config(6, 20, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_5,
        &scheduler_budget_5,
        &mut scheduler_config_5,
    );
    assert!(poll(&mut s).is_pending());
    assert_eq!(b.reading.polls(), 4);
    a.reading.open();
    assert_eq!(collect(s), (0..6).map(Ok).collect::<Vec<_>>());
}
#[test]
fn read_buffers_future_file_while_front_file_is_slow() {
    read_allows_multiple_readers_inside_window();
}
#[test]
fn read_respects_global_frame_budget() {
    let budget = FrameBudget::new(4);
    let a = Read::new(0..10);
    a.reading.close();
    let mut scheduler_files_6 = stream::iter([a]);
    let mut scheduler_config_6 = config(100, 100, 2, 0);
    let mut s = ReadDriver::new(&mut scheduler_files_6, &budget, &mut scheduler_config_6);
    assert!(poll(&mut s).is_pending());
    assert_eq!(s.granted_frames(), 4);
    assert_eq!(budget.available_capacity(), 0);
}
#[test]
fn read_rejects_early_eof() {
    let mut f = Read::new([1, 2]);
    f.count = 3;
    let mut scheduler_files_7 = stream::iter([f]);
    let scheduler_budget_7 = FrameBudget::new(3);
    let mut scheduler_config_7 = config(3, 3, 1, 0);
    let output = collect(ReadDriver::new(
        &mut scheduler_files_7,
        &scheduler_budget_7,
        &mut scheduler_config_7,
    ));
    assert_eq!(output.last(), Some(&Err(E::Contract(C::UnexpectedEof))));
}
#[test]
fn read_rejects_extra_frames() {
    let mut f = Read::new([1, 2]);
    f.count = 1;
    let mut scheduler_files_8 = stream::iter([f]);
    let scheduler_budget_8 = FrameBudget::new(1);
    let mut scheduler_config_8 = config(1, 1, 1, 0);
    let output = collect(ReadDriver::new(
        &mut scheduler_files_8,
        &scheduler_budget_8,
        &mut scheduler_config_8,
    ));
    assert_eq!(output.last(), Some(&Err(E::Contract(C::TooManyFrames))));
}
#[test]
fn read_propagates_reader_error() {
    let mut f = Read::new([1]);
    f.values = vec![Err("read")];
    let mut scheduler_files_9 = stream::iter([f.clone()]);
    let scheduler_budget_9 = FrameBudget::new(1);
    let mut scheduler_config_9 = config(1, 1, 1, 5);
    let output = collect(ReadDriver::new(
        &mut scheduler_files_9,
        &scheduler_budget_9,
        &mut scheduler_config_9,
    ));
    assert_eq!(output, [Err(E::Backend("read"))]);
    assert_eq!(f.opens.get(), 1);
}
#[test]
fn read_retries_only_open_errors() {
    let f = Read::new([1]);
    f.open_failures.set(2);
    let mut scheduler_files_10 = stream::iter([f.clone()]);
    let scheduler_budget_10 = FrameBudget::new(1);
    let mut scheduler_config_10 = config(1, 1, 1, 2);
    let output = collect(ReadDriver::new(
        &mut scheduler_files_10,
        &scheduler_budget_10,
        &mut scheduler_config_10,
    ));
    assert_eq!(output, [Ok(1)]);
    assert_eq!(f.opens.get(), 3);
}
#[test]
fn read_releases_budget_on_drop() {
    let b = FrameBudget::new(4);
    let f = Read::new(0..4);
    f.reading.close();
    let mut scheduler_files_11 = stream::iter([f]);
    let mut scheduler_config_11 = config(4, 10, 2, 0);
    let mut s = ReadDriver::new(&mut scheduler_files_11, &b, &mut scheduler_config_11);
    assert!(poll(&mut s).is_pending());
    drop(s);
    assert_eq!(b.available_capacity(), 4);
}
#[test]
fn read_drops_prefetched_unused_files() {
    let a = Read::new(0..4);
    a.reading.close();
    let b = Read::new(4..8);
    let mut scheduler_files_12 = stream::iter([a, b.clone()]);
    let scheduler_budget_12 = FrameBudget::new(4);
    let mut scheduler_config_12 = config(4, 10, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_12,
        &scheduler_budget_12,
        &mut scheduler_config_12,
    );
    assert!(poll(&mut s).is_pending());
    drop(s);
    assert_eq!(b.drops.get(), 1);
}
#[test]
fn accepts_unpin_free_inputs_and_backends() {
    let files = PinnedStream::new(stream::iter([Read::new(0..3)]));
    let mut scheduler_files_13 = std::pin::pin!(files);
    let scheduler_budget_13 = FrameBudget::new(2);
    let mut scheduler_config_13 = config(2, 10, 2, 0);
    assert_eq!(
        collect(ReadDriver::new(
            &mut scheduler_files_13,
            &scheduler_budget_13,
            &mut scheduler_config_13,
        )),
        [Ok(0), Ok(1), Ok(2)]
    );
}
#[test]
fn rejects_zero_counts_and_configuration() {
    for (f, budget, cfg, error) in [
        (Read::new([]), 1, config(1, 1, 1, 0), C::ZeroFrameCount),
        (Read::new([1]), 0, config(1, 1, 1, 0), C::ZeroBudget),
        (Read::new([1]), 1, config(0, 1, 1, 0), C::InvalidWindow),
    ] {
        let mut scheduler_files_14 = stream::iter([f]);
        let scheduler_budget_14 = FrameBudget::new(budget);
        let mut scheduler_config_14 = cfg;
        assert_eq!(
            collect(ReadDriver::new(
                &mut scheduler_files_14,
                &scheduler_budget_14,
                &mut scheduler_config_14,
            )),
            [Err(E::Contract(error))]
        );
    }
}
#[test]
fn pending_eof_blocks_following_file_until_contract_is_verified() {
    let f = Read::new([0]);
    let gate = f.reading.clone();
    let mut scheduler_files_15 = stream::iter([f, Read::new([1])]);
    let scheduler_budget_15 = FrameBudget::new(1);
    let mut scheduler_config_15 = config(1, 10, 2, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_15,
        &scheduler_budget_15,
        &mut scheduler_config_15,
    );
    // The general ordered-output test also covers EOF waiting at every boundary.
    assert_eq!(poll(&mut s), Poll::Ready(Some(Ok(0))));
    gate.open();
    assert_eq!(collect(s), [Ok(1)]);
}
#[test]
fn empty_file_stream_is_fused() {
    let mut scheduler_files_16 = stream::empty::<Read>();
    let scheduler_budget_16 = FrameBudget::new(1);
    let mut scheduler_config_16 = config(1, 1, 1, 0);
    let mut s = ReadDriver::new(
        &mut scheduler_files_16,
        &scheduler_budget_16,
        &mut scheduler_config_16,
    );
    assert_eq!(poll(&mut s), Poll::Ready(None));
    assert_eq!(poll(&mut s), Poll::Ready(None));
    assert!(s.next().now_or_never().is_some());
}
use futures::FutureExt;

#[test]
fn changing_window_preserves_buffered_future_frames() {
    let front = Read::new(0..4);
    front.reading.close();
    let future = Read::new(4..8);
    let budget = FrameBudget::new(8);
    let mut scheduler_files_17 = stream::iter([front.clone(), future.clone()]);
    let mut scheduler_config_17 = config(2, 100, 2, 0);
    let mut s = ReadDriver::new(&mut scheduler_files_17, &budget, &mut scheduler_config_17);
    assert!(poll(&mut s).is_pending());
    s.set_window(config(8, 100, 2, 0).window).unwrap();
    assert!(poll(&mut s).is_pending());
    assert_eq!(
        front.reading.polls(),
        1,
        "growing a quota must not repoll a pending reader"
    );
    assert_eq!(future.reading.polls(), 5);
    s.set_window(config(1, 100, 1, 0).window).unwrap();
    assert_eq!(
        s.granted_frames(),
        8,
        "already authorized frames survive shrinking"
    );
    front.reading.open();
    assert_eq!(collect(s), (0..8).map(Ok).collect::<Vec<_>>());
    assert_eq!(budget.available_capacity(), 8);
}
