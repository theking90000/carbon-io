use crate::{
    ContractError, FrameBudget, FramePermit, MAX_POLL_OPS, ReadFile, SchedulerConfig,
    SchedulerError, Window, ready::ReadyQueue,
};
use futures_core::{Stream, stream::FusedStream};
use pin_project_lite::pin_project;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

const BUDGET: usize = 0;
const FIRST_SLOT: usize = 1;

pin_project! {
    #[project = ReadIoProj]
    enum ReadIo<O, R> {
        Opening { #[pin] future: O },
        Ready { #[pin] reader: R },
        Idle,
    }
}

struct Slot<T, F: ReadFile<T>> {
    file: Option<F>,
    io: Pin<Box<ReadIo<F::Open, F::Reader>>>,
    start: usize,
    count: u32,
    received: u32,
    allowed: u32,
    retries: u32,
}

/// Ordered reading with anticipatory opens and a contiguous shared-budget window.
///
/// Admit owned file descriptors with `poll_file_ready` followed by `start_file`.
/// A successful readiness poll reserves one admission until its start call,
/// explicit closure or terminal failure, including across window changes.
/// `close_files` ends admission; buffered frames remain available in order.
/// Polling drives all work. No input stream or background task is retained.
/// Descriptors and frames need neither `Clone` nor `Unpin`. Backend futures and
/// readers remain pinned internally. Fatal errors release resources and are
/// reported once by the next fallible operation or stream poll.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, future::{ready, poll_fn}, ops::Range, pin::Pin};
/// use futures::{executor::block_on, stream, StreamExt};
/// use carbon_io::{FrameBudget, ReadFile, ReadScheduler, SchedulerConfig};
///
/// struct MemFile(Range<u32>);
/// impl ReadFile<u32> for MemFile {
///     type Error = Infallible;
///     type Reader = stream::Iter<std::vec::IntoIter<Result<u32, Infallible>>>;
///     type Open = std::future::Ready<Result<Self::Reader, Self::Error>>;
///     fn frame_count(&self) -> u32 { self.0.end - self.0.start }
///     fn open(&self) -> Self::Open {
///         ready(Ok(stream::iter(self.0.clone().map(Ok).collect::<Vec<_>>())))
///     }
/// }
///
/// block_on(async {
///     let budget = FrameBudget::new(16);
///     let mut config = SchedulerConfig::default();
///     let mut scheduler = ReadScheduler::new(&budget, &mut config);
///     poll_fn(|cx| Pin::new(&mut scheduler).poll_file_ready(cx)).await.unwrap();
///     Pin::new(&mut scheduler).start_file(MemFile(0..2)).unwrap();
///     Pin::new(&mut scheduler).close_files().unwrap();
///     assert_eq!(scheduler.next().await.unwrap().unwrap(), 0);
///     assert_eq!(scheduler.next().await.unwrap().unwrap(), 1);
///     assert!(scheduler.next().await.is_none());
/// });
/// ```
pub struct ReadScheduler<'a, T, F: ReadFile<T>> {
    slots: Vec<Slot<T, F>>,
    order: VecDeque<usize>,
    free: Vec<usize>,
    ready: ReadyQueue,
    waker: Option<Waker>,
    permit: FramePermit<'a>,
    config: &'a mut SchedulerConfig,
    ring: Vec<Option<T>>,
    head: usize,
    emitted: usize,
    discovered: usize,
    authorized: usize,
    allow_cursor: usize,
    active: usize,
    error: Option<SchedulerError<F::Error>>,
    files_closed: bool,
    file_ready: bool,
    terminated: bool,
}

impl<T, F: ReadFile<T>> fmt::Debug for ReadScheduler<'_, T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadScheduler")
            .field("window", &self.config.window)
            .field("granted_frames", &self.granted_frames())
            .field("active_files", &self.active)
            .field("emitted", &self.emitted)
            .field("discovered", &self.discovered)
            .field("authorized", &self.authorized)
            .field("files_closed", &self.files_closed)
            .field("file_ready", &self.file_ready)
            .field("terminated", &self.terminated)
            .finish()
    }
}

// Only boxed backend I/O states are structurally pinned. T and F remain movable.
impl<T, F: ReadFile<T>> Unpin for ReadScheduler<'_, T, F> {}

impl<'a, T, F: ReadFile<T>> ReadScheduler<'a, T, F> {
    /// Borrow the shared budget and mutable configuration, without input streams.
    /// Initial configuration errors are reported by the first fallible operation
    /// or stream poll. `set_window` updates the borrowed configuration.
    pub fn new(budget: &'a FrameBudget, config: &'a mut SchedulerConfig) -> Self {
        let mut ready = ReadyQueue::new();
        ready.add();
        ready.schedule(BUDGET);
        let error = config
            .window
            .validate()
            .err()
            .or_else(|| (budget.total_capacity() == 0).then_some(ContractError::ZeroBudget))
            .map(Into::into);
        Self {
            slots: Vec::new(),
            order: VecDeque::new(),
            free: Vec::new(),
            ready,
            waker: None,
            permit: budget.permit(),
            config,
            ring: Vec::new(),
            head: 0,
            emitted: 0,
            discovered: 0,
            authorized: 0,
            allow_cursor: 0,
            active: 0,
            error,
            files_closed: false,
            file_ready: false,
            terminated: false,
        }
    }
    /// Change the window while preserving authorized frames, opened files and
    /// outstanding file admission. Returns `InvalidWindow` for zero target frames
    /// or zero active-file limit. Poll again to drive the updated configuration.
    pub fn set_window(&mut self, window: Window) -> Result<(), ContractError> {
        window.validate()?;
        self.config.window = window;
        self.permit.shrink_to(
            self.permit.capacity().min(
                window
                    .target_frames
                    .max(self.authorized.wrapping_sub(self.emitted)),
            ),
        );
        self.ready.schedule(BUDGET);
        self.wake();
        Ok(())
    }
    /// Current window configuration.
    pub fn window(&self) -> Window {
        self.config.window
    }
    /// Locally granted capacity, without synchronizing with the shared budget.
    pub fn granted_frames(&self) -> usize {
        self.permit.capacity()
    }

    fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
    fn reject(&mut self, error: ContractError) -> SchedulerError<F::Error> {
        self.finish();
        self.wake();
        error.into()
    }
    fn adjust_capacity(&mut self) {
        let keep = self
            .config
            .window
            .target_frames
            .max(self.authorized.wrapping_sub(self.emitted));
        let excess = self.permit.capacity().saturating_sub(keep);
        if excess >= 32 || (excess > 0 && keep == self.config.window.target_frames) {
            self.permit.shrink_to(keep);
        }
    }
    fn grow(&mut self, cx: &mut Context<'_>) {
        let target = self
            .config
            .window
            .target_frames
            .min(self.permit.total_capacity());
        if self.permit.capacity() < target {
            let minimum = (self.permit.capacity() + target.min(32)).min(target);
            if self.permit.poll_grow(cx, minimum, target).is_pending() {
                return;
            }
        }
        if self.ring.len() < self.permit.capacity() {
            self.ring.rotate_left(self.head);
            self.head = 0;
            self.ring.resize_with(self.permit.capacity(), || None);
        }
        self.allow();
        if self.permit.capacity() < target {
            self.ready.schedule(BUDGET);
        }
    }
    fn allow(&mut self) {
        let mut space = self
            .permit
            .capacity()
            .min(self.config.window.target_frames)
            .saturating_sub(self.authorized.wrapping_sub(self.emitted));
        while space > 0 && self.allow_cursor < self.order.len() {
            let id = self.order[self.allow_cursor];
            let slot = &mut self.slots[id];
            let extra = space.min((slot.count - slot.allowed) as usize);
            let quota_blocked = slot.received == slot.allowed;
            slot.allowed += extra as u32;
            self.authorized = self.authorized.wrapping_add(extra);
            space -= extra;
            if quota_blocked && extra > 0 && matches!(*slot.io, ReadIo::Ready { .. }) {
                self.ready.schedule(id + FIRST_SLOT);
            }
            if slot.allowed == slot.count {
                self.allow_cursor += 1;
            } else {
                break;
            }
        }
    }
    /// Drive I/O and reserve one file admission. A file may extend beyond the
    /// discovery horizon if its beginning is inside it. An existing reservation
    /// survives progress and subsequent window reductions.
    /// Returns `InputClosed` after closure or a reported terminal error.
    pub fn poll_file_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SchedulerError<F::Error>>> {
        let this = self.get_mut();
        this.drive(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Err(error));
        }
        if this.terminated || this.files_closed {
            return Poll::Ready(Err(this.reject(ContractError::InputClosed)));
        }
        if this.file_ready {
            return Poll::Ready(Ok(()));
        }
        if this.active >= this.config.window.max_active_files
            || this.discovered.wrapping_sub(this.emitted) >= this.config.window.horizon()
        {
            return Poll::Pending;
        }
        this.file_ready = true;
        Poll::Ready(Ok(()))
    }

    /// Transfer ownership of a file after a successful `poll_file_ready`.
    /// Starts its opening attempt; backend polling stays in the common I/O engine.
    /// Missing readiness, closed admission and zero frame counts return terminal
    /// `AdmissionNotReady`, `InputClosed` and `ZeroFrameCount` contract errors.
    pub fn start_file(self: Pin<&mut Self>, file: F) -> Result<(), SchedulerError<F::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.error.take() {
            this.finish();
            this.wake();
            return Err(error);
        }
        if this.terminated || this.files_closed {
            return Err(this.reject(ContractError::InputClosed));
        }
        if !this.file_ready {
            return Err(this.reject(ContractError::AdmissionNotReady));
        }
        this.file_ready = false;
        this.admit(file)
    }

    /// Close file admission and cancel its outstanding reservation.
    /// Already accepted files continue reading and their frames remain ordered.
    /// Closure is idempotent. The output ends after all accepted files are drained
    /// and their readers have confirmed EOF.
    pub fn close_files(self: Pin<&mut Self>) -> Result<(), SchedulerError<F::Error>> {
        let this = self.get_mut();
        if let Some(error) = this.error.take() {
            this.finish();
            this.wake();
            return Err(error);
        }
        this.files_closed = true;
        this.file_ready = false;
        this.wake();
        Ok(())
    }

    fn admit(&mut self, file: F) -> Result<(), SchedulerError<F::Error>> {
        let count = file.frame_count();
        if count == 0 {
            return Err(self.reject(ContractError::ZeroFrameCount));
        }
        let opening = ReadIo::Opening {
            future: file.open(),
        };
        let id = if let Some(id) = self.free.pop() {
            let slot = &mut self.slots[id];
            slot.file = Some(file);
            slot.io.set(opening);
            slot.start = self.discovered;
            slot.count = count;
            slot.received = 0;
            slot.allowed = 0;
            slot.retries = 0;
            id
        } else {
            let id = self.slots.len();
            self.slots.push(Slot {
                file: Some(file),
                io: Box::pin(opening),
                start: self.discovered,
                count,
                received: 0,
                allowed: 0,
                retries: 0,
            });
            self.ready.add();
            id
        };
        self.discovered = self.discovered.wrapping_add(count as usize);
        self.active += 1;
        self.order.push_back(id);
        self.ready.schedule(id + FIRST_SLOT);
        self.allow();
        self.wake();
        Ok(())
    }
    fn poll_slot(
        &mut self,
        id: usize,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<F::Error>> {
        let slot = &mut self.slots[id];
        if slot.file.is_none() {
            return Ok(());
        }
        match slot.io.as_mut().project() {
            ReadIoProj::Opening { future } => match future.poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(reader)) => {
                    slot.io.set(ReadIo::Ready { reader });
                    self.ready.schedule(id + FIRST_SLOT);
                }
                Poll::Ready(Err(error)) => {
                    slot.io.set(ReadIo::Idle);
                    if slot.retries == self.config.max_retries {
                        return Err(SchedulerError::Backend(error));
                    }
                    slot.retries += 1;
                    let Some(file) = slot.file.as_ref() else {
                        return Err(ContractError::UnexpectedEof.into());
                    };
                    slot.io.set(ReadIo::Opening {
                        future: file.open(),
                    });
                    self.ready.schedule(id + FIRST_SLOT);
                }
            },
            ReadIoProj::Ready { reader } => {
                if slot.received == slot.allowed && slot.received < slot.count {
                    return Ok(());
                }
                match reader.poll_next(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Some(Err(error))) => return Err(SchedulerError::Backend(error)),
                    Poll::Ready(Some(Ok(frame))) => {
                        if slot.received == slot.count {
                            return Err(ContractError::TooManyFrames.into());
                        }
                        let offset = slot
                            .start
                            .wrapping_add(slot.received as usize)
                            .wrapping_sub(self.emitted);
                        let index = (self.head + offset) % self.ring.len();
                        debug_assert!(self.ring[index].is_none());
                        self.ring[index] = Some(frame);
                        slot.received += 1;
                        self.ready.schedule(id + FIRST_SLOT);
                    }
                    Poll::Ready(None) => {
                        if slot.received != slot.count {
                            return Err(ContractError::UnexpectedEof.into());
                        }
                        slot.io.set(ReadIo::Idle);
                        self.active -= 1;
                        self.wake();
                    }
                }
            }
            ReadIoProj::Idle => {}
        }
        Ok(())
    }
    fn take_frame(&mut self) -> Option<T> {
        self.retire();
        if let Some(&id) = self.order.front() {
            let slot = &self.slots[id];
            if self.emitted == slot.start.wrapping_add(slot.count as usize) {
                return None;
            }
        }
        let frame = self.ring.get_mut(self.head)?.take()?;
        self.head = (self.head + 1) % self.ring.len();
        self.emitted = self.emitted.wrapping_add(1);
        if self.discovered.wrapping_sub(self.emitted) < self.config.window.horizon() {
            self.wake();
        }
        self.retire();
        self.adjust_capacity();
        self.allow();
        Some(frame)
    }
    fn retire(&mut self) {
        while let Some(&id) = self.order.front() {
            let slot = &mut self.slots[id];
            if self.emitted != slot.start.wrapping_add(slot.count as usize)
                || !matches!(*slot.io, ReadIo::Idle)
            {
                break;
            }
            slot.file = None;
            self.order.pop_front();
            self.allow_cursor = self.allow_cursor.saturating_sub(1);
            self.free.push(id);
            self.wake();
        }
    }
    /// Advance I/O without consuming any output, processing at most 256 work items.
    ///
    /// Registers the current task for backend and budget wakeups. If runnable work
    /// remains after this turn, wakes the task again. A full window or pending I/O
    /// does not cause a busy loop. The caller must keep polling to make progress.
    /// Fatal errors release resources immediately and remain available to the next
    /// stream poll. Calling this after completion or failure does nothing.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) {
        self.drive(cx);
    }

    fn drive(&mut self, cx: &mut Context<'_>) {
        if self.terminated {
            return;
        }
        if self
            .waker
            .as_ref()
            .is_none_or(|old| !old.will_wake(cx.waker()))
        {
            self.waker = Some(cx.waker().clone());
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
        if self.files_closed && self.order.is_empty() {
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
    fn poll_work(&mut self, cx: &mut Context<'_>) -> Result<(), SchedulerError<F::Error>> {
        self.ready.register(cx.waker());
        for _ in 0..MAX_POLL_OPS {
            let Some(id) = self.ready.pop() else { break };
            let waker = self.ready.waker(id);
            let mut child = Context::from_waker(&waker);
            let result = match id {
                BUDGET => {
                    self.grow(&mut child);
                    Ok(())
                }
                _ => self.poll_slot(id - FIRST_SLOT, &mut child),
            };
            result?;
        }
        self.retire();
        Ok(())
    }

    fn finish(&mut self) {
        self.files_closed = true;
        self.file_ready = false;
        self.slots.clear();
        self.order.clear();
        self.free.clear();
        self.active = 0;
        self.allow_cursor = 0;
        self.ring.clear();
        self.permit.shrink_to(0);
        self.terminated = true;
    }
}

impl<T, F: ReadFile<T>> Stream for ReadScheduler<'_, T, F> {
    type Item = Result<T, SchedulerError<F::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(this.error.take().map(Err));
        }
        if let Some(error) = this.error.take() {
            this.finish();
            return Poll::Ready(Some(Err(error)));
        }
        // Drain a previously filled window without locks, atomics, or slot scans.
        if let Some(frame) = this.take_frame() {
            return Poll::Ready(Some(Ok(frame)));
        }
        this.drive(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if this.terminated {
            return Poll::Ready(None);
        }
        if let Some(frame) = this.take_frame() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if this.files_closed && this.order.is_empty() {
            this.finish();
            return Poll::Ready(None);
        }
        if this.ready.has_work() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}
impl<T, F: ReadFile<T>> FusedStream for ReadScheduler<'_, T, F> {
    fn is_terminated(&self) -> bool {
        self.terminated && self.error.is_none()
    }
}
