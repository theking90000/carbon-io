//! Read scheduler admission, backpressure, closure, and ownership tests.
mod support;

use carbon_io::{ContractError as C, FrameBudget, ReadFile, ReadScheduler, SchedulerError as E};
use futures::{stream, stream::FusedStream};
use std::{convert::Infallible, future::{Ready, ready}, marker::PhantomPinned, pin::Pin, task::{Context, Poll}};
use support::*;

#[test]
fn open_input_waits_until_explicit_close() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 4, 2, 0);
    let mut reader = ReadScheduler::<usize, Read>::new(&budget, &mut cfg);
    assert!(poll(&mut reader).is_pending());
    assert!(!reader.is_terminated());
    Pin::new(&mut reader).close_files().unwrap();
    assert_eq!(poll(&mut reader), Poll::Ready(None));
    assert!(reader.is_terminated());
    Pin::new(&mut reader).close_files().unwrap();
    assert_eq!(budget.available_capacity(), 2);
}

#[test]
fn granted_file_survives_progress_and_window_shrink() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 10, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let first = Read::new(0..4);
    first.reading.close();
    admit_read_file(&mut reader, first.clone());
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut reader).poll_file_ready(&mut cx), Poll::Ready(Ok(())));
    reader.set_window(config(1, 1, 1, 0).window).unwrap();
    reader.poll_progress(&mut cx);
    assert_eq!(Pin::new(&mut reader).poll_file_ready(&mut cx), Poll::Ready(Ok(())));
    let second = Read::new(4..6);
    Pin::new(&mut reader).start_file(second.clone()).unwrap();
    assert_eq!(second.opens.get(), 1);
    assert!(Pin::new(&mut reader).poll_file_ready(&mut cx).is_pending());
    Pin::new(&mut reader).close_files().unwrap();
    first.reading.open();
    assert_eq!(collect(reader), (0..6).map(Ok).collect::<Vec<_>>());
}

#[test]
fn file_horizon_blocks_until_enough_frames_are_consumed() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 3, 5, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    admit_read_file(&mut reader, Read::new(0..5));
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    for value in 0..3 {
        assert!(Pin::new(&mut reader).poll_file_ready(&mut cx).is_pending());
        assert_eq!(poll(&mut reader), Poll::Ready(Some(Ok(value))));
    }
    admit_read_file(&mut reader, Read::new(5..8));
    Pin::new(&mut reader).close_files().unwrap();
    assert_eq!(collect(reader), (3..8).map(Ok).collect::<Vec<_>>());
}

#[test]
fn active_limit_reopens_admission_after_file_retirement() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 20, 1, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let first = Read::new([0]);
    first.reading.close();
    admit_read_file(&mut reader, first.clone());
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut reader).poll_file_ready(&mut cx).is_pending());
    first.reading.open();
    assert_eq!(poll(&mut reader), Poll::Ready(Some(Ok(0))));
    admit_read_file(&mut reader, Read::new([1]));
    Pin::new(&mut reader).close_files().unwrap();
    assert_eq!(collect(reader), [Ok(1)]);
}

#[test]
fn rejected_admissions_return_errors_without_opening_the_file() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 4, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let file = Read::new([0]);
    assert_eq!(Pin::new(&mut reader).start_file(file.clone()), Err(E::Contract(C::AdmissionNotReady)));
    assert_eq!(file.opens.get(), 0);
    assert_eq!(poll(&mut reader), Poll::Ready(None));
    assert_eq!(budget.available_capacity(), 2);
    drop(reader);

    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut reader).poll_file_ready(&mut cx), Poll::Ready(Ok(())));
    Pin::new(&mut reader).close_files().unwrap();
    assert_eq!(Pin::new(&mut reader).start_file(file.clone()), Err(E::Contract(C::InputClosed)));
    assert_eq!(file.opens.get(), 0);
    assert_eq!(Pin::new(&mut reader).poll_file_ready(&mut cx), Poll::Ready(Err(E::Contract(C::InputClosed))));
}

#[test]
fn backend_error_invalidates_grant_and_is_delivered_once() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 10, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let mut first = Read::new([0]);
    first.values = vec![Err("read")];
    first.reading.close();
    admit_read_file(&mut reader, first.clone());
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut reader).poll_file_ready(&mut cx), Poll::Ready(Ok(())));
    first.reading.open();
    reader.poll_progress(&mut cx);
    let second = Read::new([1]);
    assert_eq!(Pin::new(&mut reader).start_file(second.clone()), Err(E::Backend("read")));
    assert_eq!(second.opens.get(), 0);
    assert_eq!(first.drops.get(), 1);
    assert_eq!(budget.available_capacity(), 2);
    assert_eq!(poll(&mut reader), Poll::Ready(None));
}

struct OwnedFile(Read, PhantomPinned);

impl ReadFile<usize> for OwnedFile {
    type Error = <Read as ReadFile<usize>>::Error;
    type Open = <Read as ReadFile<usize>>::Open;
    type Reader = <Read as ReadFile<usize>>::Reader;

    fn frame_count(&self) -> u32 { self.0.frame_count() }
    fn open(&self) -> Self::Open { self.0.open() }
}

#[test]
fn owned_descriptors_need_neither_clone_nor_unpin_and_open_ahead() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 10, 2, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    let first = Read::new(0..3);
    first.reading.close();
    let second = Read::new(3..5);
    admit_read_file(&mut reader, OwnedFile(first.clone(), PhantomPinned));
    admit_read_file(&mut reader, OwnedFile(second.clone(), PhantomPinned));
    Pin::new(&mut reader).close_files().unwrap();
    assert!(poll(&mut reader).is_pending());
    assert_eq!(second.opens.get(), 1);
    assert_eq!(second.opening.polls(), 1);
    assert_eq!(second.reading.polls(), 0);
    first.reading.open();
    assert_eq!(collect(reader), (0..5).map(Ok).collect::<Vec<_>>());
}

struct OwnedFrame(usize, PhantomPinned);
struct FrameSource;

impl ReadFile<OwnedFrame> for FrameSource {
    type Error = Infallible;
    type Reader = stream::Iter<std::vec::IntoIter<Result<OwnedFrame, Infallible>>>;
    type Open = Ready<Result<Self::Reader, Infallible>>;

    fn frame_count(&self) -> u32 { 2 }
    fn open(&self) -> Self::Open {
        ready(Ok(stream::iter(vec![Ok(OwnedFrame(0, PhantomPinned)), Ok(OwnedFrame(1, PhantomPinned))])))
    }
}

#[test]
fn output_frames_need_neither_clone_nor_unpin() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 2, 1, 0);
    let mut reader = ReadScheduler::new(&budget, &mut cfg);
    admit_read_file(&mut reader, FrameSource);
    Pin::new(&mut reader).close_files().unwrap();
    let values: Vec<_> = collect(reader).into_iter().map(|frame| frame.unwrap().0).collect();
    assert_eq!(values, [0, 1]);
}
