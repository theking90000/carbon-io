//! Direct admission, buffer reuse, ordered authorization, and pinned files.
mod support;

use carbon_io::{ContractError, FrameBudget, PushError, ReadFile, ReadScheduler};
use std::{
    cell::Cell,
    marker::PhantomPinned,
    pin::Pin,
    rc::Rc,
    sync::atomic::Ordering,
    task::{Context, Poll},
};
use support::*;

#[test]
fn empty_input_requires_explicit_repoll_after_push_and_close() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 4, 2, 0);
    let mut allocations = 0;
    let mut make_frame = || {
        allocations += 1;
        0
    };
    let mut reader = ReadScheduler::new(&mut make_frame, &budget, &mut cfg).unwrap();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let mut output = 99;
    assert_eq!(reader.poll_read(&mut cx, &mut output), Poll::Pending);
    reader.poll_progress(&mut cx);
    assert_eq!(reader.granted_frames(), 0);
    assert_eq!(budget.available_capacity(), 2);
    let file = Read::new([7]);
    assert!(reader.push_file(file.clone()).is_ok());
    assert_eq!(file.opens.get(), 0);
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(Some(())))
    );
    assert_eq!(output, 7);
    assert_eq!(reader.poll_read(&mut cx, &mut output), Poll::Pending);
    reader.close_files();
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(None))
    );
    assert_eq!(output, 7);
    drop(reader);
    assert_eq!(allocations, 1);
}

#[test]
fn refusals_return_the_file_and_leave_the_scheduler_usable() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 3, 1, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(matches!(
        reader.push_file(Read::new([])),
        Err((PushError::Contract(ContractError::ZeroFrameCount), _))
    ));
    assert!(reader.push_file(Read::new([0])).is_ok());
    let file = Read::new([1]);
    let Err((PushError::Full, file)) = reader.push_file(file) else {
        panic!("expected full window");
    };
    assert_eq!(file.opens.get(), 0);
    assert_eq!(poll_read(&mut reader), Poll::Ready(Some(Ok(0))));
    let (_, waker) = context_waker();
    reader.poll_progress(&mut Context::from_waker(&waker));
    assert!(reader.push_file(file.clone()).is_ok());
    reader.close_files();
    assert!(matches!(
        reader.push_file(Read::new([2])),
        Err((PushError::Closed, _))
    ));
    assert_eq!(collect_read(&mut reader), [Ok(1)]);
}

#[test]
fn open_ahead_does_not_authorize_out_of_order_buffering() {
    let budget = FrameBudget::new(10);
    let mut cfg = config(10, 120, 2, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let a = Read::new(0..100);
    let b = Read::new(100..200);
    assert!(reader.push_file(a).is_ok());
    assert!(reader.push_file(b.clone()).is_ok());
    reader.close_files();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    reader.poll_progress(&mut cx);
    assert_eq!(b.opens.get(), 1);
    assert_eq!(b.reading.polls(), 0);
    let mut output = 0;
    for value in 0..91 {
        assert_eq!(
            reader.poll_read(&mut cx, &mut output),
            Poll::Ready(Ok(Some(())))
        );
        assert_eq!(output, value);
        assert_eq!(b.reading.polls(), 0);
    }
    reader.poll_progress(&mut cx);
    assert_eq!(b.reading.polls(), 1);
    assert_eq!(
        collect_read(&mut reader),
        (91..200).map(Ok).collect::<Vec<_>>()
    );
}

#[test]
fn later_files_finish_before_the_head_but_output_stays_ordered() {
    let budget = FrameBudget::new(6);
    let mut cfg = config(6, 20, 2, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let a = Read::new(0..3);
    a.reading.close();
    let b = Read::new(3..6);
    assert!(reader.push_file(a.clone()).is_ok());
    assert!(reader.push_file(b.clone()).is_ok());
    reader.close_files();
    assert!(poll_read(&mut reader).is_pending());
    assert_eq!(b.reading.polls(), 3);
    a.reading.open();
    assert_eq!(
        collect_read(&mut reader),
        (0..6).map(Ok).collect::<Vec<_>>()
    );
}

#[test]
fn progress_does_not_consume_or_exceed_the_budget() {
    let budget = FrameBudget::new(4);
    let mut cfg = config(4, 20, 2, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let file = Read::new(0..10);
    assert!(reader.push_file(file.clone()).is_ok());
    reader.close_files();
    let (wakes, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    reader.poll_progress(&mut cx);
    assert_eq!(file.reading.polls(), 4);
    let before = wakes.0.load(Ordering::Relaxed);
    for _ in 0..4 {
        reader.poll_progress(&mut cx);
    }
    assert_eq!(file.reading.polls(), 4);
    assert_eq!(wakes.0.load(Ordering::Relaxed), before);
    assert_eq!(reader.granted_frames(), 4);
    assert_eq!(
        collect_read(&mut reader),
        (0..10).map(Ok).collect::<Vec<_>>()
    );
    assert_eq!(budget.available_capacity(), 4);
}

#[derive(Default)]
struct Buffer(Vec<u8>);
struct Source {
    gate: Gate,
    started: bool,
    emitted: bool,
    pointer: Rc<Cell<usize>>,
}
impl ReadFile<Buffer> for Source {
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
        frame: &mut Buffer,
    ) -> Poll<Result<Option<()>, ()>> {
        let this = self.get_mut();
        if this.emitted {
            return Poll::Ready(Ok(None));
        }
        if !this.started {
            frame.0.clear();
            frame.0.push(1);
            this.pointer.set(frame.0.as_ptr() as usize);
            this.started = true;
        }
        if !this.gate.ready(cx) {
            return Poll::Pending;
        }
        assert_eq!(frame.0, [1]);
        assert_eq!(frame.0.as_ptr() as usize, this.pointer.get());
        frame.0.push(2);
        this.emitted = true;
        Poll::Ready(Ok(Some(())))
    }
}

#[test]
fn pending_preserves_contents_and_complete_output_swaps_allocations() {
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 1, 1, 0);
    let mut allocations = 0;
    let mut make_frame = || {
        allocations += 1;
        Buffer(Vec::with_capacity(8))
    };
    let gate = Gate::new(false);
    let pointer = Rc::new(Cell::new(0));
    let mut reader = ReadScheduler::new(&mut make_frame, &budget, &mut cfg).unwrap();
    assert!(
        reader
            .push_file(Source {
                gate: gate.clone(),
                started: false,
                emitted: false,
                pointer: pointer.clone()
            })
            .is_ok()
    );
    reader.close_files();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let mut output = Buffer(vec![9]);
    assert!(reader.poll_read(&mut cx, &mut output).is_pending());
    assert_eq!(output.0, [9]);
    assert!(reader.poll_read(&mut cx, &mut output).is_pending());
    gate.open();
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(Some(())))
    );
    assert_eq!(output.0, [1, 2]);
    assert_eq!(output.0.as_ptr() as usize, pointer.get());
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(None))
    );
    drop(reader);
    assert_eq!(allocations, 1);
}

#[test]
fn declared_count_ignores_extra_data_with_later_frames_buffered() {
    let budget = FrameBudget::new(2);
    let mut cfg = config(2, 20, 2, 0);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    let mut a = Read::new([0]);
    a.values = vec![Ok(0), Ok(999)];
    assert!(reader.push_file(a.clone()).is_ok());
    assert!(reader.push_file(Read::new(1..10)).is_ok());
    reader.close_files();
    assert_eq!(poll_read(&mut reader), Poll::Ready(Some(Ok(0))));
    assert_eq!(
        collect_read(&mut reader),
        (1..10).map(Ok).collect::<Vec<_>>()
    );
    assert_eq!(a.reading.polls(), 1);
    assert_eq!(budget.available_capacity(), 2);
}

pin_project_lite::pin_project! { struct PinnedFile { inner: Read, #[pin] _pin: PhantomPinned } }
impl ReadFile<usize> for PinnedFile {
    type Error = &'static str;
    fn frame_count(&self) -> u32 {
        self.inner.frame_count()
    }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(self.project().inner).poll_open(cx)
    }
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &mut usize,
    ) -> Poll<Result<Option<()>, Self::Error>> {
        Pin::new(self.project().inner).poll_read(cx, frame)
    }
}

#[test]
fn pinned_files_and_non_unpin_frames_need_no_clone() {
    struct Frame(usize, PhantomPinned);
    struct File(bool);
    impl ReadFile<Frame> for File {
        type Error = ();
        fn frame_count(&self) -> u32 {
            1
        }
        fn poll_open(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            frame: &mut Frame,
        ) -> Poll<Result<Option<()>, ()>> {
            if self.0 {
                return Poll::Ready(Ok(None));
            }
            frame.0 = 7;
            self.get_mut().0 = true;
            Poll::Ready(Ok(Some(())))
        }
    }
    let budget = FrameBudget::new(1);
    let mut cfg = config(1, 10, 1, 0);
    let mut reader = ReadScheduler::new(|| Frame(0, PhantomPinned), &budget, &mut cfg).unwrap();
    assert!(reader.push_file(File(false)).is_ok());
    reader.close_files();
    let (_, waker) = context_waker();
    let mut output = Frame(0, PhantomPinned);
    assert_eq!(
        reader.poll_read(&mut Context::from_waker(&waker), &mut output),
        Poll::Ready(Ok(Some(())))
    );
    assert_eq!(output.0, 7);
    drop(reader);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(
        reader
            .push_file(Box::pin(PinnedFile {
                inner: Read::new([8]),
                _pin: PhantomPinned
            }))
            .is_ok()
    );
    reader.close_files();
    assert_eq!(collect_read(&mut reader), [Ok(8)]);
}

#[test]
fn replacing_window_cancels_an_old_pending_budget_request() {
    let budget = FrameBudget::new(64);
    let mut owner = budget.permit();
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(owner.poll_grow(&mut cx, 64, 64).is_ready());
    let mut cfg = config(64, 100, 1, 0);
    let file = Read::new([7]);
    let mut reader = ReadScheduler::new(|| 0, &budget, &mut cfg).unwrap();
    assert!(reader.push_file(file).is_ok());
    reader.close_files();
    reader.poll_progress(&mut cx);
    reader.set_window(config(1, 100, 1, 0).window).unwrap();
    reader.poll_progress(&mut cx);
    owner.shrink_to(63);
    let mut output = 0;
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(Some(())))
    );
    assert_eq!(output, 7);
    assert_eq!(
        reader.poll_read(&mut cx, &mut output),
        Poll::Ready(Ok(None))
    );
}
