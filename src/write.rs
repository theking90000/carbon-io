use crate::{
    ContractError, FrameBudget, FramePermit, FrameWriter, MAX_POLL_OPS, SchedulerConfig,
    SchedulerError, Window, WriteFile, ready::ReadyQueue,
};
use futures_core::{Stream, stream::FusedStream};
use pin_project_lite::pin_project;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

const FILES: usize = 0;
const INPUT: usize = 1;
const BUDGET: usize = 2;
const FIRST_SLOT: usize = 3;

pin_project! {
    #[project = WriteIoProj]
    enum WriteIo<O, W, R> {
        Opening { #[pin] future: O },
        Ready { #[pin] writer: W },
        Done { result: Option<R> },
        Idle,
    }
}

enum Frames<T> {
    Replay(Vec<T>),
    Streaming(VecDeque<T>),
}
impl<T> Frames<T> {
    fn new(replay: bool) -> Self {
        if replay {
            Self::Replay(Vec::new())
        } else {
            Self::Streaming(VecDeque::new())
        }
    }
    fn push(&mut self, frame: T) {
        match self {
            Self::Replay(v) => v.push(frame),
            Self::Streaming(v) => v.push_back(frame),
        }
    }
    fn next(&self, written: u32) -> Option<&T> {
        match self {
            Self::Replay(v) => v.get(written as usize),
            Self::Streaming(v) => v.front(),
        }
    }
    fn accepted(&mut self) {
        if let Self::Streaming(v) = self {
            v.pop_front();
        }
    }
    fn clear(&mut self) {
        match self {
            Self::Replay(v) => v.clear(),
            Self::Streaming(v) => v.clear(),
        }
    }
}
type FileIo<T, F> =
    WriteIo<<F as WriteFile<T>>::Open, <F as WriteFile<T>>::Writer, <F as WriteFile<T>>::Output>;

struct Slot<T, F: WriteFile<T>> {
    file: Option<F>,
    io: Pin<Box<FileIo<T, F>>>,
    frames: Frames<T>,
    capacity: u32,
    assigned: u32,
    written: u32,
    retries: u32,
    pending: bool,
}

/// Ordered destination results with concurrent writes and optional full-file replay.
///
/// With `max_retries == 0`, accepted frames are dropped immediately. Otherwise a
/// whole destination is reserved before accepting its first frame, and all its
/// frames survive until successful finalization. Opening unused destinations may
/// have backend side effects; the backend must clean up abandoned attempts.
pub struct WriteScheduler<T, I, S>
where
    I: Stream<Item = T>,
    S: Stream,
    S::Item: WriteFile<T>,
{
    input: Option<Pin<Box<I>>>,
    files: Option<Pin<Box<S>>>,
    slots: Vec<Slot<T, S::Item>>,
    order: VecDeque<usize>,
    free: Vec<usize>,
    fill_index: usize,
    ready: ReadyQueue,
    permit: FramePermit,
    config: SchedulerConfig,
    active: usize,
    horizon: usize,
    reserved: usize,
    retained: usize,
    // A reservation for the current file exists before its first input poll.
    fill_reserved: bool,
    input_ready: bool,
    files_ready: bool,
    budget_waiting: bool,
    error: Option<SchedulerError<<S::Item as WriteFile<T>>::Error>>,
    terminated: bool,
}
impl<T, I, S> Unpin for WriteScheduler<T, I, S>
where
    I: Stream<Item = T>,
    S: Stream,
    S::Item: WriteFile<T>,
{
}

impl<T, I, S> WriteScheduler<T, I, S>
where
    I: Stream<Item = T>,
    S: Stream,
    S::Item: WriteFile<T>,
{
    /// Build a scheduler. Initial contract errors are emitted by the stream.
    pub fn new(input: I, files: S, budget: FrameBudget, config: SchedulerConfig) -> Self {
        let mut ready = ReadyQueue::new();
        for _ in 0..FIRST_SLOT {
            ready.add();
        }
        ready.schedule(FILES);
        ready.schedule(INPUT);
        let error = config
            .window
            .validate()
            .err()
            .or_else(|| (budget.total_capacity() == 0).then_some(ContractError::ZeroBudget))
            .map(Into::into);
        Self {
            input: Some(Box::pin(input)),
            files: Some(Box::pin(files)),
            slots: Vec::new(),
            order: VecDeque::new(),
            free: Vec::new(),
            fill_index: 0,
            ready,
            permit: budget.permit(),
            config,
            active: 0,
            horizon: 0,
            reserved: 0,
            retained: 0,
            fill_reserved: false,
            input_ready: true,
            files_ready: true,
            budget_waiting: false,
            error,
            terminated: false,
        }
    }
    /// Replace the window without discarding accepted frames or existing files.
    pub fn set_window(&mut self, window: Window) -> Result<(), ContractError> {
        window.validate()?;
        self.config.window = window;
        self.permit.shrink_to(
            self.permit
                .capacity()
                .min(window.target_frames.max(self.engaged())),
        );
        self.budget_waiting = false;
        self.schedule_input();
        self.schedule_files();
        Ok(())
    }
    /// Current window configuration.
    pub fn window(&self) -> Window {
        self.config.window
    }
    /// Locally granted capacity.
    pub fn granted_frames(&self) -> usize {
        self.permit.capacity()
    }
    /// Number of frames currently owned by the scheduler.
    pub fn retained_frames(&self) -> usize {
        self.retained
    }
    fn replay(&self) -> bool {
        self.config.max_retries > 0
    }
    fn engaged(&self) -> usize {
        if self.replay() {
            self.reserved
        } else {
            self.retained
        }
    }
    fn schedule_input(&mut self) {
        if self.input_ready && !self.budget_waiting {
            self.ready.schedule(INPUT);
        }
    }
    fn schedule_files(&mut self) {
        if self.files_ready {
            self.ready.schedule(FILES);
        }
    }
    fn schedule_slot(&mut self, id: usize) {
        if !self.slots[id].pending {
            self.ready.schedule(id + FIRST_SLOT);
        }
    }
    fn discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as WriteFile<T>>::Error>> {
        if self.input.is_none()
            || self.active >= self.config.window.max_active_files
            || self.horizon >= self.config.window.horizon()
        {
            return Ok(());
        }
        let Some(files) = self.files.as_mut() else {
            return Ok(());
        };
        let file = match files.as_mut().poll_next(cx) {
            Poll::Pending => {
                self.files_ready = false;
                return Ok(());
            }
            Poll::Ready(None) => {
                self.files = None;
                self.schedule_input();
                return Ok(());
            }
            Poll::Ready(Some(file)) => file,
        };
        let capacity = file.frame_capacity();
        if capacity == 0 {
            return Err(ContractError::ZeroFrameCapacity.into());
        }
        if self.replay() && capacity as usize > self.permit.total_capacity() {
            return Err(ContractError::FrameCapacityExceedsBudget.into());
        }
        let opening = WriteIo::Opening {
            future: file.open(),
        };
        let id = if let Some(id) = self.free.pop() {
            let slot = &mut self.slots[id];
            slot.file = Some(file);
            slot.io.set(opening);
            slot.frames.clear();
            slot.capacity = capacity;
            slot.assigned = 0;
            slot.written = 0;
            slot.retries = 0;
            slot.pending = false;
            id
        } else {
            let id = self.slots.len();
            self.slots.push(Slot {
                file: Some(file),
                io: Box::pin(opening),
                frames: Frames::new(self.replay()),
                capacity,
                assigned: 0,
                written: 0,
                retries: 0,
                pending: false,
            });
            let signal_id = self.ready.add();
            debug_assert_eq!(signal_id, id + FIRST_SLOT);
            id
        };
        self.horizon = self.horizon.saturating_add(capacity as usize);
        self.active += 1;
        self.order.push_back(id);
        self.ready.schedule(id + FIRST_SLOT);
        self.schedule_files();
        self.schedule_input();
        Ok(())
    }
    fn ensure_capacity(&mut self, minimum: usize, cx: &mut Context<'_>) -> bool {
        if minimum > self.permit.total_capacity() {
            return false;
        }
        if self.permit.capacity() >= minimum {
            return true;
        }
        if self.budget_waiting {
            return false;
        }
        // Do not hold unused fragments while waiting for a complete replay file.
        let engaged = self.engaged();
        if self.permit.capacity() > engaged {
            self.permit.shrink_to(engaged);
        }
        let target = self
            .config
            .window
            .target_frames
            .max(minimum)
            .min(self.permit.total_capacity());
        let minimum = if self.replay() {
            minimum
        } else {
            (engaged + target.min(32)).min(target).max(minimum)
        };
        let waker = self.ready.waker(BUDGET);
        let mut budget_cx = Context::from_waker(&waker);
        let _ = cx;
        self.budget_waiting = self
            .permit
            .poll_grow(&mut budget_cx, minimum, target)
            .is_pending();
        !self.budget_waiting
    }
    fn poll_input(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as WriteFile<T>>::Error>> {
        if self.input.is_none() {
            return Ok(());
        }
        if !self.replay() && self.retained >= self.config.window.target_frames {
            return Ok(());
        }
        let destination = self.order.get(self.fill_index).copied();
        if destination.is_none() && self.files.is_some() {
            return Ok(());
        }
        if let Some(id) = destination {
            if self.replay() && !self.fill_reserved {
                let capacity = self.slots[id].capacity as usize;
                // Reservations cannot exceed the global budget. Wait for a prior
                // file's finalization before requesting another complete file.
                if capacity > self.permit.total_capacity() - self.reserved {
                    return Ok(());
                }
                if !self.ensure_capacity(self.reserved + capacity, cx) {
                    return Ok(());
                }
                self.reserved += capacity;
                self.fill_reserved = true;
            } else if !self.replay() && !self.ensure_capacity(self.retained + 1, cx) {
                return Ok(());
            }
        } else if !self.ensure_capacity(self.engaged() + 1, cx) {
            return Ok(());
        }
        match self.input.as_mut().unwrap().as_mut().poll_next(cx) {
            Poll::Pending => {
                self.input_ready = false;
            }
            Poll::Ready(Some(frame)) => {
                let Some(id) = destination else {
                    return Err(ContractError::MissingWriteFile.into());
                };
                let slot = &mut self.slots[id];
                slot.frames.push(frame);
                slot.assigned += 1;
                self.retained += 1;
                if slot.assigned == slot.capacity {
                    self.fill_index += 1;
                    self.fill_reserved = false;
                }
                self.schedule_slot(id);
                self.schedule_input();
            }
            Poll::Ready(None) => self.input_eof(),
        }
        Ok(())
    }
    fn input_eof(&mut self) {
        self.input = None;
        self.files = None;
        // Only the current partial file can need an EOF-induced finalization.
        if let Some(&id) = self.order.get(self.fill_index) {
            if self.slots[id].assigned > 0 {
                self.schedule_slot(id);
            } else if self.fill_reserved {
                self.reserved -= self.slots[id].capacity as usize;
            }
        }
        self.fill_reserved = false;
        while let Some(&id) = self.order.back() {
            if self.slots[id].assigned > 0 {
                break;
            }
            self.slots[id].io.set(WriteIo::Idle);
            self.slots[id].file = None;
            self.active -= 1;
            self.horizon -= self.slots[id].capacity as usize;
            self.order.pop_back();
            self.free.push(id);
        }
        self.permit.shrink_to(self.engaged());
        self.budget_waiting = false;
    }
    fn poll_slot(
        &mut self,
        id: usize,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as WriteFile<T>>::Error>> {
        let replay = self.replay();
        let slot = &mut self.slots[id];
        if slot.file.is_none() {
            return Ok(());
        }
        slot.pending = false;
        let error = match slot.io.as_mut().project() {
            WriteIoProj::Opening { future } => match future.poll(cx) {
                Poll::Pending => {
                    slot.pending = true;
                    None
                }
                Poll::Ready(Ok(writer)) => {
                    slot.io.set(WriteIo::Ready { writer });
                    self.ready.schedule(id + FIRST_SLOT);
                    None
                }
                Poll::Ready(Err(error)) => Some(error),
            },
            WriteIoProj::Ready { writer } => {
                if let Some(frame) = slot.frames.next(slot.written) {
                    match writer.poll_write(cx, frame) {
                        Poll::Pending => {
                            slot.pending = true;
                            None
                        }
                        Poll::Ready(Err(error)) => Some(error),
                        Poll::Ready(Ok(())) => {
                            slot.written += 1;
                            slot.frames.accepted();
                            if !replay {
                                self.retained -= 1;
                                let keep = self.config.window.target_frames.max(self.retained);
                                let excess = self.permit.capacity().saturating_sub(keep);
                                if excess >= 32
                                    || (excess > 0
                                        && self.retained <= self.config.window.target_frames)
                                {
                                    self.permit.shrink_to(keep);
                                }
                            }
                            self.ready.schedule(id + FIRST_SLOT);
                            self.schedule_input();
                            None
                        }
                    }
                } else if slot.assigned == slot.capacity
                    || (self.input.is_none() && slot.assigned > 0)
                {
                    match writer.poll_finalize(cx) {
                        Poll::Pending => {
                            slot.pending = true;
                            None
                        }
                        Poll::Ready(Err(error)) => Some(error),
                        Poll::Ready(Ok(result)) => {
                            slot.io.set(WriteIo::Done {
                                result: Some(result),
                            });
                            if replay {
                                self.retained -= slot.assigned as usize;
                                self.reserved -= slot.capacity as usize;
                            }
                            slot.frames.clear();
                            self.active -= 1;
                            // Finalization is a file boundary, so returning unused
                            // capacity here never puts a global lock on each frame.
                            self.permit.shrink_to(self.engaged());
                            self.budget_waiting = false;
                            self.schedule_input();
                            self.schedule_files();
                            None
                        }
                    }
                } else {
                    None
                }
            }
            WriteIoProj::Done { .. } | WriteIoProj::Idle => None,
        };
        if let Some(error) = error {
            let slot = &mut self.slots[id];
            slot.io.set(WriteIo::Idle);
            if slot.retries == self.config.max_retries {
                return Err(SchedulerError::Backend(error));
            }
            slot.retries += 1;
            slot.written = 0;
            slot.io.set(WriteIo::Opening {
                future: slot.file.as_ref().unwrap().open(),
            });
            self.ready.schedule(id + FIRST_SLOT);
        }
        Ok(())
    }
    fn take_result(&mut self) -> Option<<S::Item as WriteFile<T>>::Output> {
        let &id = self.order.front()?;
        let slot = &mut self.slots[id];
        let WriteIoProj::Done { result } = slot.io.as_mut().project() else {
            return None;
        };
        let result = result.take();
        self.horizon -= slot.capacity as usize;
        slot.io.set(WriteIo::Idle);
        slot.file = None;
        self.order.pop_front();
        self.free.push(id);
        self.fill_index = self.fill_index.saturating_sub(1);
        self.schedule_files();
        self.schedule_input();
        result
    }
    /// Advance I/O without consuming any output, processing at most 256 work items.
    ///
    /// Registers the current task for backend and budget wakeups. If runnable work
    /// remains after this turn, wakes the task again. A full window or pending I/O
    /// does not cause a busy loop. The caller must keep polling to make progress.
    /// Fatal errors release resources immediately and remain available to the next
    /// stream poll. Calling this after completion or failure does nothing.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) {
        if self.terminated {
            return;
        }
        if self.error.is_some() {
            self.finish();
            return;
        }
        if let Err(error) = self.poll_work(cx) {
            self.finish();
            self.error = Some(error);
            return;
        }
        if self.input.is_none() && self.order.is_empty() {
            self.finish();
            return;
        }
        if self.ready.has_work() {
            cx.waker().wake_by_ref();
        }
    }

    /// Keep I/O progressing without consuming output. This future never completes.
    ///
    /// Use alongside another future in `select!`. This stays pending even at EOF
    /// or on error. Once no runnable work remains, it waits for wakeups. Errors
    /// remain available to the next stream poll. No task or buffer is created.
    ///
    /// Dropping this future leaves scheduler state intact, including pending I/O,
    /// buffered output, and errors. Dropping the scheduler itself abandons its I/O
    /// and releases its frames and permits.
    pub async fn progress(&mut self) {
        std::future::poll_fn(|cx| {
            self.poll_progress(cx);
            Poll::<()>::Pending
        })
        .await
    }

    // Keep wakeups out of poll_next's ready-output path.
    #[inline]
    fn poll_work(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as WriteFile<T>>::Error>> {
        self.ready.register(cx.waker());
        for _ in 0..MAX_POLL_OPS {
            let Some(id) = self.ready.pop() else { break };
            let waker = self.ready.waker(id);
            let mut child = Context::from_waker(&waker);
            let result = match id {
                FILES => {
                    self.files_ready = true;
                    self.discover(&mut child)
                }
                INPUT => {
                    self.input_ready = true;
                    self.poll_input(&mut child)
                }
                BUDGET => {
                    self.budget_waiting = false;
                    self.schedule_input();
                    Ok(())
                }
                _ => self.poll_slot(id - FIRST_SLOT, &mut child),
            };
            result?;
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.input = None;
        self.files = None;
        self.slots.clear();
        self.order.clear();
        self.permit.shrink_to(0);
        self.retained = 0;
        self.reserved = 0;
        self.terminated = true;
    }
}

impl<T, I, S> Stream for WriteScheduler<T, I, S>
where
    I: Stream<Item = T>,
    S: Stream,
    S::Item: WriteFile<T>,
{
    type Item =
        Result<<S::Item as WriteFile<T>>::Output, SchedulerError<<S::Item as WriteFile<T>>::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(this.error.take().map(Err));
        }
        if let Some(error) = this.error.take() {
            this.finish();
            return Poll::Ready(Some(Err(error)));
        }
        if let Some(result) = this.take_result() {
            return Poll::Ready(Some(Ok(result)));
        }
        if let Err(error) = this.poll_work(cx) {
            this.finish();
            return Poll::Ready(Some(Err(error)));
        }
        if let Some(result) = this.take_result() {
            return Poll::Ready(Some(Ok(result)));
        }
        if this.input.is_none() && this.order.is_empty() {
            this.finish();
            return Poll::Ready(None);
        }
        if this.ready.has_work() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}
impl<T, I, S> FusedStream for WriteScheduler<T, I, S>
where
    I: Stream<Item = T>,
    S: Stream,
    S::Item: WriteFile<T>,
{
    fn is_terminated(&self) -> bool {
        self.terminated && self.error.is_none()
    }
}
