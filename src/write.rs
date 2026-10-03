use crate::{
    ContractError, FrameBudget, FramePermit, FrameWriter, MAX_POLL_OPS, SchedulerConfig,
    SchedulerError, Window, WriteFile, ready::ReadyQueue,
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

/// Input admission or an ordered finalization result from a [`WriteScheduler`].
#[derive(Debug, PartialEq, Eq)]
pub enum WriteEvent<R> {
    /// Destination admission is available. Use `accept_file` and `enqueue_file`
    /// to fill the window. The first admission remains reserved until consumed.
    FileReady,
    /// Frame admission is available. Use `accept_frame` and `enqueue_frame` to
    /// fill the budget. The first admission remains reserved until consumed.
    FrameReady,
    /// The next destination's finalization result, transferred to the caller.
    File(R),
}

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

/// Push destinations and owned frames, then pull ordered destination results.
///
/// Each input has a readiness/admission pair. Poll readiness before transferring
/// ownership with `enqueue_file` or `enqueue_frame`. A successful readiness poll
/// reserves one admission until its corresponding start call, explicit input
/// closure, or a terminal error. Repeated readiness polls reuse that reservation.
/// All polling interfaces drive the same I/O engine without consuming results.
///
/// Destinations open only after their first frame. With retries disabled, a
/// successful backend write releases its frame. Otherwise the scheduler reserves
/// the whole destination and retains its frames until successful finalization.
/// Neither frames nor destination descriptors need `Clone` or `Unpin`.
///
/// Close both inputs to end the result stream. Closing frames finalizes the last
/// used partial destination and drops unused descriptors without opening them.
/// Results must be consumed to advance the file discovery horizon. Fatal errors
/// release resources and are reported once by the next fallible operation or
/// stream poll. Dropping the scheduler cancels I/O and returns all permits.
///
/// # Examples
///
/// ```
/// use std::{convert::Infallible, future::ready, pin::Pin, task::{Context, Poll}};
/// use futures::{executor::block_on, StreamExt};
/// use carbon_io::{FrameBudget, FrameWriter, SchedulerConfig, WriteFile, WriteScheduler};
///
/// struct MemFile;
/// struct MemWriter(u32);
/// impl WriteFile<u32> for MemFile {
///     type Error = Infallible;
///     type Output = u32;
///     type Open = std::future::Ready<Result<MemWriter, Infallible>>;
///     type Writer = MemWriter;
///     fn frame_capacity(&self) -> u32 { 2 }
///     fn open(&self) -> Self::Open { ready(Ok(MemWriter(0))) }
/// }
/// impl FrameWriter<u32> for MemWriter {
///     type Error = Infallible;
///     type Output = u32;
///     fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, frame: &u32) -> Poll<Result<(), Infallible>> {
///         self.get_mut().0 += *frame;
///         Poll::Ready(Ok(()))
///     }
///     fn poll_finalize(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<u32, Infallible>> {
///         Poll::Ready(Ok(self.0))
///     }
/// }
///
/// block_on(async {
///     let budget = FrameBudget::new(16);
///     let mut config = SchedulerConfig::default();
///     let mut scheduler = WriteScheduler::new(&budget, &mut config);
///     scheduler.file_ready().await.unwrap();
///     scheduler.enqueue_file(MemFile).unwrap();
///     scheduler.close_files().unwrap();
///     for frame in [10, 20] {
///         scheduler.frame_ready().await.unwrap();
///         scheduler.enqueue_frame(frame).unwrap();
///     }
///     scheduler.close_frames().unwrap();
///     assert_eq!(scheduler.next().await.unwrap().unwrap(), 30);
///     assert!(scheduler.next().await.is_none());
/// });
/// ```
pub struct WriteScheduler<'a, T, F: WriteFile<T>> {
    slots: Vec<Slot<T, F>>,
    order: VecDeque<usize>,
    free: Vec<usize>,
    fill_index: usize,
    ready: ReadyQueue,
    waker: Option<Waker>,
    permit: FramePermit<'a>,
    config: &'a mut SchedulerConfig,
    active: usize,
    horizon: usize,
    reserved: usize,
    retained: usize,
    // Replay capacity for the destination currently being filled.
    fill_reserved: bool,
    // Outstanding admissions survive progress, finalization and window changes.
    file_ready: bool,
    frame_ready: Option<usize>,
    event_cursor: usize,
    frames_closed: bool,
    files_closed: bool,
    budget_waiting: bool,
    error: Option<SchedulerError<F::Error>>,
    terminated: bool,
}

impl<T, F: WriteFile<T>> fmt::Debug for WriteScheduler<'_, T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteScheduler")
            .field("window", &self.config.window)
            .field("max_retries", &self.config.max_retries)
            .field("granted_frames", &self.granted_frames())
            .field("retained_frames", &self.retained_frames())
            .field("active_files", &self.active)
            .field("horizon", &self.horizon)
            .field("file_ready", &self.file_ready)
            .field("frame_ready", &self.frame_ready.is_some())
            .field("files_closed", &self.files_closed)
            .field("frames_closed", &self.frames_closed)
            .field("terminated", &self.terminated)
            .finish()
    }
}

// Only the boxed I/O states are structurally pinned. T and F remain movable.
impl<T, F: WriteFile<T>> Unpin for WriteScheduler<'_, T, F> {}

impl<'a, T, F: WriteFile<T>> WriteScheduler<'a, T, F> {
    /// Borrow the shared budget and mutable configuration, without input streams.
    /// Initial configuration errors are reported by the first fallible operation
    /// or stream poll. `set_window` updates the borrowed configuration.
    pub fn new(budget: &'a FrameBudget, config: &'a mut SchedulerConfig) -> Self {
        let mut ready = ReadyQueue::new();
        ready.add();
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
            fill_index: 0,
            ready,
            waker: None,
            permit: budget.permit(),
            config,
            active: 0,
            horizon: 0,
            reserved: 0,
            retained: 0,
            fill_reserved: false,
            file_ready: false,
            frame_ready: None,
            event_cursor: 0,
            frames_closed: false,
            files_closed: false,
            budget_waiting: false,
            error,
            terminated: false,
        }
    }

    /// Drive I/O and reserve one destination admission.
    /// The next file may extend beyond the horizon if its beginning is inside it.
    /// An existing admission survives subsequent window reductions.
    ///
    /// # Panics
    /// Panics when either input is closed or after a terminal error was reported.
    pub fn poll_file_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SchedulerError<F::Error>>> {
        let this = self;
        this.drive(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Err(error));
        }
        assert!(
            !this.terminated && !this.files_closed && !this.frames_closed,
            "file admission is closed"
        );
        this.reserve_file().map(Ok)
    }

    fn reserve_file(&mut self) -> Poll<()> {
        if self.file_ready {
            return Poll::Ready(());
        }
        if self.active >= self.config.window.max_active_files
            || self.horizon >= self.config.window.horizon()
        {
            return Poll::Pending;
        }
        self.file_ready = true;
        Poll::Ready(())
    }

    /// Wait for and reserve one file admission, driving I/O while pending.
    /// Dropping this future preserves the scheduler and any granted admission.
    pub async fn file_ready(&mut self) -> Result<(), SchedulerError<F::Error>> {
        std::future::poll_fn(|cx| self.poll_file_ready(cx)).await
    }

    /// Reserve one destination admission without polling backend I/O.
    /// Returns `false` on backpressure, closure or terminal failure. Errors remain
    /// available through the next fallible operation. Repeated calls preserve the
    /// same admission until `enqueue_file` consumes it or admission closes.
    pub fn accept_file(&mut self) -> bool {
        !self.terminated
            && self.error.is_none()
            && !self.files_closed
            && !self.frames_closed
            && self.reserve_file().is_ready()
    }

    /// Transfer a destination reserved by `accept_file`, a readiness poll
    /// or a `FileReady` event.
    /// Invalid capacities are terminal contract errors. The destination remains
    /// unopened until its first frame arrives.
    ///
    /// # Panics
    /// Panics without a reserved admission, after closure or a reported error.
    pub fn enqueue_file(&mut self, file: F) -> Result<(), SchedulerError<F::Error>> {
        let this = self;
        this.check_error()?;
        assert!(
            this.file_ready && !this.files_closed && !this.frames_closed,
            "enqueue_file requires a successful poll_file_ready"
        );
        this.file_ready = false;
        let capacity = file.frame_capacity();
        let error = if capacity == 0 {
            Some(ContractError::ZeroFrameCapacity)
        } else if this.replay() && capacity as usize > this.permit.total_capacity() {
            Some(ContractError::FrameCapacityExceedsBudget)
        } else {
            None
        };
        if let Some(error) = error {
            this.finish();
            this.wake();
            return Err(error.into());
        }
        let id = if let Some(id) = this.free.pop() {
            let slot = &mut this.slots[id];
            slot.file = Some(file);
            slot.io.set(WriteIo::Idle);
            slot.frames.clear();
            slot.capacity = capacity;
            slot.assigned = 0;
            slot.written = 0;
            slot.retries = 0;
            slot.pending = false;
            id
        } else {
            let id = this.slots.len();
            this.slots.push(Slot {
                file: Some(file),
                io: Box::pin(WriteIo::Idle),
                frames: Frames::new(this.replay()),
                capacity,
                assigned: 0,
                written: 0,
                retries: 0,
                pending: false,
            });
            this.ready.add();
            id
        };
        this.horizon = this.horizon.saturating_add(capacity as usize);
        this.active += 1;
        this.order.push_back(id);
        this.wake();
        Ok(())
    }

    /// Drive I/O and reserve one frame admission, without receiving a frame.
    /// No destination returns `Pending` while files remain open, or a terminal
    /// `MissingWriteFile` error after files close. The reservation survives other
    /// polls and window changes until `enqueue_frame`, closure or terminal failure.
    /// With retries enabled, the first admission reserves the whole destination.
    ///
    /// # Panics
    /// Panics when frames are closed or after a terminal error was reported.
    pub fn poll_frame_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), SchedulerError<F::Error>>> {
        let this = self;
        this.drive(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Err(error));
        }
        assert!(
            !this.terminated && !this.frames_closed,
            "frame admission is closed"
        );
        this.reserve_frame()
    }

    fn reserve_frame(&mut self) -> Poll<Result<(), SchedulerError<F::Error>>> {
        let this = self;
        if this.frame_ready.is_some() {
            return Poll::Ready(Ok(()));
        }
        let Some(&id) = this.order.get(this.fill_index) else {
            if this.files_closed {
                this.finish();
                this.wake();
                return Poll::Ready(Err(ContractError::MissingWriteFile.into()));
            }
            return Poll::Pending;
        };
        if this.replay() {
            if !this.fill_reserved {
                let capacity = this.slots[id].capacity as usize;
                if capacity > this.permit.total_capacity() - this.reserved
                    || !this.ensure_capacity(this.reserved + capacity)
                {
                    return Poll::Pending;
                }
                this.reserved += capacity;
                this.fill_reserved = true;
            }
        } else {
            if this.retained >= this.config.window.target_frames
                || !this.ensure_capacity(this.retained + 1)
            {
                return Poll::Pending;
            }
        }
        this.frame_ready = Some(id);
        Poll::Ready(Ok(()))
    }

    /// Wait for and reserve one frame admission, driving I/O while pending.
    /// Dropping this future preserves the scheduler and budget reservations.
    pub async fn frame_ready(&mut self) -> Result<(), SchedulerError<F::Error>> {
        std::future::poll_fn(|cx| self.poll_frame_ready(cx)).await
    }

    /// Reserve one frame admission without polling backend I/O.
    /// Returns `false` when no destination or budget capacity is available, or
    /// after closure or terminal failure. Errors remain available through the
    /// next fallible operation. Repeated calls preserve the same reservation
    /// until `enqueue_frame` consumes it or frame admission closes.
    pub fn accept_frame(&mut self) -> bool {
        if self.terminated
            || self.error.is_some()
            || self.frames_closed
            || (self.frame_ready.is_none() && self.order.get(self.fill_index).is_none())
        {
            return false;
        }
        match self.reserve_frame() {
            Poll::Ready(Ok(())) => true,
            Poll::Pending => false,
            Poll::Ready(Err(error)) => {
                self.error = Some(error);
                false
            }
        }
    }

    /// Drive I/O and return an input admission or an ordered finalization result.
    /// Rotates file, frame and output priority when multiple events are ready.
    /// Handle readiness with the matching `enqueue_*` or `close_*` call. Close an
    /// input as soon as its producer ends, without waiting for another admission.
    /// Returns `None` after both inputs close and results drain, or an error once.
    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<WriteEvent<F::Output>>, SchedulerError<F::Error>>> {
        self.drive(cx);
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }
        if self.terminated {
            return Poll::Ready(Ok(None));
        }
        for offset in 0..3 {
            match (self.event_cursor + offset) % 3 {
                0 if !self.files_closed
                    && !self.frames_closed
                    && self.reserve_file().is_ready() =>
                {
                    self.event_cursor = 1;
                    return Poll::Ready(Ok(Some(WriteEvent::FileReady)));
                }
                1 if !self.frames_closed
                    && (self.frame_ready.is_some()
                        || self.order.get(self.fill_index).is_some()) =>
                {
                    match self.reserve_frame() {
                        Poll::Ready(Ok(())) => {
                            self.event_cursor = 2;
                            return Poll::Ready(Ok(Some(WriteEvent::FrameReady)));
                        }
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => {}
                    }
                }
                2 => {
                    if let Some(result) = self.take_result() {
                        self.event_cursor = 0;
                        return Poll::Ready(Ok(Some(WriteEvent::File(result))));
                    }
                }
                _ => {}
            }
        }
        Poll::Pending
    }

    /// Wait for input readiness, a finalization result, error or EOF with one borrow.
    /// Dropping a pending future preserves the scheduler and outstanding admissions.
    pub async fn next_event(
        &mut self,
    ) -> Result<Option<WriteEvent<F::Output>>, SchedulerError<F::Error>> {
        std::future::poll_fn(|cx| self.poll(cx)).await
    }

    /// Transfer one owned frame into an admission reserved by `accept_frame`,
    /// a readiness poll or a `FrameReady` event.
    /// Success means the scheduler owns the frame, not that backend I/O finished.
    ///
    /// # Panics
    /// Panics without a reserved admission, after closure or a reported error.
    pub fn enqueue_frame(&mut self, frame: T) -> Result<(), SchedulerError<F::Error>> {
        let this = self;
        this.check_error()?;
        assert!(!this.frames_closed, "frame admission is closed");
        let id = this
            .frame_ready
            .take()
            .expect("enqueue_frame requires a successful poll_frame_ready");
        let slot = &mut this.slots[id];
        slot.frames.push(frame);
        slot.assigned += 1;
        this.retained += 1;
        if slot.assigned == 1 {
            let Some(file) = slot.file.as_ref() else {
                this.finish();
                this.wake();
                return Err(ContractError::MissingWriteFile.into());
            };
            slot.io.set(WriteIo::Opening {
                future: file.open(),
            });
        }
        if slot.assigned == slot.capacity {
            this.fill_index += 1;
            this.fill_reserved = false;
        }
        this.schedule_slot(id);
        this.wake();
        Ok(())
    }

    /// Close destination admission and cancel its outstanding readiness grant.
    /// Already announced destinations remain available to frame admission.
    /// Closure is idempotent and does not close frames.
    pub fn close_files(&mut self) -> Result<(), SchedulerError<F::Error>> {
        let this = self;
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

    /// Close frame admission and cancel its outstanding readiness grant.
    /// Finalize the last used partial destination and discard unused descriptors.
    /// Closure is idempotent; close files separately to end the result stream.
    pub fn close_frames(&mut self) -> Result<(), SchedulerError<F::Error>> {
        let this = self;
        if let Some(error) = this.error.take() {
            this.finish();
            this.wake();
            return Err(error);
        }
        if this.frames_closed {
            return Ok(());
        }
        this.frames_closed = true;
        this.frame_ready = None;
        this.file_ready = false;
        if let Some(&id) = this.order.get(this.fill_index) {
            if this.slots[id].assigned > 0 {
                this.schedule_slot(id);
            } else if this.fill_reserved {
                this.reserved -= this.slots[id].capacity as usize;
            }
        }
        this.fill_reserved = false;
        while let Some(&id) = this.order.back() {
            if this.slots[id].assigned > 0 {
                break;
            }
            this.slots[id].io.set(WriteIo::Idle);
            this.slots[id].file = None;
            this.active -= 1;
            this.horizon -= this.slots[id].capacity as usize;
            this.order.pop_back();
            this.free.push(id);
        }
        this.permit.shrink_to(this.engaged());
        this.budget_waiting = false;
        this.wake();
        Ok(())
    }

    /// Replace the window while preserving accepted frames and ready admissions.
    /// Returns `InvalidWindow` for zero target frames or zero active-file limit.
    pub fn set_window(&mut self, window: Window) -> Result<(), ContractError> {
        window.validate()?;
        self.config.window = window;
        self.permit.shrink_to(
            self.permit
                .capacity()
                .min(window.target_frames.max(self.engaged())),
        );
        self.budget_waiting = false;
        self.wake();
        Ok(())
    }

    /// Current window configuration.
    pub fn window(&self) -> Window {
        self.config.window
    }
    /// Locally granted capacity, including outstanding admissions and replay.
    pub fn granted_frames(&self) -> usize {
        self.permit.capacity()
    }
    /// Number of frames currently owned by the scheduler, excluding ready grants.
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
            self.retained + usize::from(self.frame_ready.is_some())
        }
    }
    fn wake(&self) {
        if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }
    fn check_error(&mut self) -> Result<(), SchedulerError<F::Error>> {
        if let Some(error) = self.error.take() {
            self.finish();
            return Err(error);
        }
        assert!(!self.terminated, "scheduler has terminated");
        Ok(())
    }
    fn schedule_slot(&mut self, id: usize) {
        if !self.slots[id].pending {
            self.ready.schedule(id + FIRST_SLOT);
        }
    }
    fn ensure_capacity(&mut self, minimum: usize) -> bool {
        if self.permit.capacity() >= minimum {
            return true;
        }
        if self.budget_waiting {
            return false;
        }
        // Return unused fragments before waiting for an entire replay file.
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
        let mut cx = Context::from_waker(&waker);
        self.budget_waiting = self.permit.poll_grow(&mut cx, minimum, target).is_pending();
        !self.budget_waiting
    }
    fn poll_slot(
        &mut self,
        id: usize,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<F::Error>> {
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
                                let keep = self.config.window.target_frames.max(self.engaged());
                                let excess = self.permit.capacity().saturating_sub(keep);
                                if excess >= 32
                                    || (excess > 0
                                        && self.retained <= self.config.window.target_frames)
                                {
                                    self.permit.shrink_to(keep);
                                }
                            }
                            self.ready.schedule(id + FIRST_SLOT);
                            self.wake();
                            None
                        }
                    }
                } else if slot.assigned == slot.capacity
                    || (self.frames_closed && slot.assigned > 0)
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
                            self.wake();
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
            let Some(file) = slot.file.as_ref() else {
                return Err(ContractError::MissingWriteFile.into());
            };
            slot.io.set(WriteIo::Opening {
                future: file.open(),
            });
            self.ready.schedule(id + FIRST_SLOT);
        }
        Ok(())
    }
    fn take_result(&mut self) -> Option<F::Output> {
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
        self.wake();
        result
    }

    /// Advance at most 256 work items without consuming a result or any input.
    /// Backend and budget wakeups schedule another poll. Fatal errors release
    /// resources and remain available to the next fallible operation or stream poll.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) {
        self.drive(cx);
    }

    /// Keep I/O progressing without providing input or consuming results.
    /// This future never completes and creates no task or channel. Dropping it
    /// preserves scheduler state; dropping the scheduler itself cancels all I/O.
    pub async fn progress(&mut self) {
        std::future::poll_fn(|cx| {
            self.poll_progress(cx);
            Poll::<()>::Pending
        })
        .await
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
            self.wake();
            return;
        }
        if self.frames_closed && self.files_closed && self.order.is_empty() {
            self.finish();
            return;
        }
        if self.ready.has_work() {
            cx.waker().wake_by_ref();
        }
    }

    #[inline]
    fn poll_work(&mut self, cx: &mut Context<'_>) -> Result<(), SchedulerError<F::Error>> {
        self.ready.register(cx.waker());
        for _ in 0..MAX_POLL_OPS {
            let Some(id) = self.ready.pop() else { break };
            if id == BUDGET {
                self.budget_waiting = false;
            } else {
                let waker = self.ready.waker(id);
                let mut child = Context::from_waker(&waker);
                self.poll_slot(id - FIRST_SLOT, &mut child)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.frames_closed = true;
        self.files_closed = true;
        self.file_ready = false;
        self.frame_ready = None;
        self.fill_reserved = false;
        self.slots.clear();
        self.order.clear();
        self.free.clear();
        self.permit.shrink_to(0);
        self.retained = 0;
        self.reserved = 0;
        self.active = 0;
        self.horizon = 0;
        self.fill_index = 0;
        self.budget_waiting = false;
        self.terminated = true;
    }
}

impl<T, F: WriteFile<T>> Stream for WriteScheduler<'_, T, F> {
    type Item = Result<F::Output, SchedulerError<F::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.drive(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if this.terminated {
            return Poll::Ready(None);
        }
        if let Some(result) = this.take_result() {
            return Poll::Ready(Some(Ok(result)));
        }
        Poll::Pending
    }
}

impl<T, F: WriteFile<T>> FusedStream for WriteScheduler<'_, T, F> {
    fn is_terminated(&self) -> bool {
        self.terminated && self.error.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{StreamExt, executor::block_on};
    use std::{
        cell::{Cell, RefCell},
        future::{Ready, ready},
        marker::PhantomPinned,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::Wake,
    };

    struct Frame {
        value: u32,
        drops: Rc<Cell<usize>>,
        _pin: PhantomPinned,
    }
    impl Frame {
        fn new(value: u32, drops: &Rc<Cell<usize>>) -> Self {
            Self {
                value,
                drops: drops.clone(),
                _pin: PhantomPinned,
            }
        }
    }
    impl Drop for Frame {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Failure {
        Open,
        Write,
        Finalize,
    }

    #[derive(Clone, Default)]
    struct Gate(Rc<RefCell<(bool, Option<Waker>)>>);
    impl Gate {
        fn close(&self) {
            self.0.borrow_mut().0 = true;
        }
        fn open(&self) {
            let waker = {
                let mut state = self.0.borrow_mut();
                state.0 = false;
                state.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
            let mut state = self.0.borrow_mut();
            if state.0 {
                state.1 = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    #[derive(Clone)]
    struct File {
        capacity: u32,
        opens: Rc<Cell<usize>>,
        finalized: Rc<Cell<usize>>,
        writing: Gate,
        finalizing: Gate,
        open_failures: Rc<Cell<usize>>,
        write_failures: Rc<Cell<usize>>,
        finalize_failures: Rc<Cell<usize>>,
        _pin: PhantomPinned,
    }
    impl File {
        fn new(capacity: u32) -> Self {
            Self {
                capacity,
                opens: Rc::new(Cell::new(0)),
                finalized: Rc::new(Cell::new(0)),
                writing: Gate::default(),
                finalizing: Gate::default(),
                open_failures: Rc::new(Cell::new(0)),
                write_failures: Rc::new(Cell::new(0)),
                finalize_failures: Rc::new(Cell::new(0)),
                _pin: PhantomPinned,
            }
        }
    }
    struct Writer {
        file: File,
        values: Vec<u32>,
    }
    // The descriptor is deliberately !Unpin, but is never structurally pinned.
    impl Unpin for Writer {}
    impl WriteFile<Frame> for File {
        type Error = Failure;
        type Output = Vec<u32>;
        type Open = Ready<Result<Writer, Failure>>;
        type Writer = Writer;

        fn frame_capacity(&self) -> u32 {
            self.capacity
        }
        fn open(&self) -> Self::Open {
            self.opens.set(self.opens.get() + 1);
            if self.open_failures.get() > 0 {
                self.open_failures.set(self.open_failures.get() - 1);
                return ready(Err(Failure::Open));
            }
            ready(Ok(Writer {
                file: self.clone(),
                values: Vec::new(),
            }))
        }
    }
    impl FrameWriter<Frame> for Writer {
        type Error = Failure;
        type Output = Vec<u32>;

        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            frame: &Frame,
        ) -> Poll<Result<(), Failure>> {
            let this = self.get_mut();
            if this.file.writing.poll(cx).is_pending() {
                return Poll::Pending;
            }
            if this.file.write_failures.get() > 0 {
                this.file
                    .write_failures
                    .set(this.file.write_failures.get() - 1);
                return Poll::Ready(Err(Failure::Write));
            }
            this.values.push(frame.value);
            Poll::Ready(Ok(()))
        }
        fn poll_finalize(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<Vec<u32>, Failure>> {
            let this = self.get_mut();
            if this.file.finalizing.poll(cx).is_pending() {
                return Poll::Pending;
            }
            if this.file.finalize_failures.get() > 0 {
                this.file
                    .finalize_failures
                    .set(this.file.finalize_failures.get() - 1);
                return Poll::Ready(Err(Failure::Finalize));
            }
            this.file.finalized.set(this.file.finalized.get() + 1);
            Poll::Ready(Ok(std::mem::take(&mut this.values)))
        }
    }

    type Scheduler<'a> = WriteScheduler<'a, Frame, File>;

    fn config(target: usize, horizon: usize, active: usize, retries: u32) -> SchedulerConfig {
        SchedulerConfig::new(Window::new(target, horizon, active).unwrap(), retries)
    }
    fn file(s: &mut Scheduler<'_>, cx: &mut Context<'_>, file: File) {
        assert!(matches!(
            Pin::new(&mut *s).poll_file_ready(cx),
            Poll::Ready(Ok(()))
        ));
        Pin::new(s).enqueue_file(file).unwrap();
    }
    fn frame(s: &mut Scheduler<'_>, cx: &mut Context<'_>, value: u32, drops: &Rc<Cell<usize>>) {
        assert!(matches!(
            Pin::new(&mut *s).poll_frame_ready(cx),
            Poll::Ready(Ok(()))
        ));
        Pin::new(s).enqueue_frame(Frame::new(value, drops)).unwrap();
    }
    fn close(s: &mut Scheduler<'_>) {
        Pin::new(&mut *s).close_files().unwrap();
        Pin::new(s).close_frames().unwrap();
    }
    fn results(s: &mut Scheduler<'_>) -> Vec<Vec<u32>> {
        block_on(s.map(Result::unwrap).collect())
    }

    #[test]
    fn frame_admission_waits_for_a_destination() {
        let budget = FrameBudget::new(4);
        let mut config = config(4, 8, 2, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut s).poll_frame_ready(&mut cx).is_pending());
        assert_eq!(budget.available_capacity(), 4);
        file(&mut s, &mut cx, File::new(2));
        assert!(matches!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(s.retained_frames(), 0);
        close(&mut s);
        assert_eq!(budget.available_capacity(), 4);
        assert!(results(&mut s).is_empty());
    }

    #[test]
    fn budget_waiter_wakes_and_reserves_before_ownership_transfer() {
        struct Counter(AtomicUsize);
        impl Wake for Counter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        let budget = FrameBudget::new(1);
        let mut held = budget.permit();
        assert!(held.poll_grow(&mut cx, 1, 1).is_ready());
        let mut config = config(1, 8, 2, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        file(&mut s, &mut cx, File::new(1));
        assert!(Pin::new(&mut s).poll_frame_ready(&mut cx).is_pending());
        let wakes = counter.0.load(Ordering::Relaxed);
        held.shrink_to(0);
        assert!(counter.0.load(Ordering::Relaxed) > wakes);
        assert!(matches!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(s.granted_frames(), 1);
        assert_eq!(budget.available_capacity(), 0);
        let drops = Rc::new(Cell::new(0));
        Pin::new(&mut s)
            .enqueue_frame(Frame::new(7, &drops))
            .unwrap();
        assert_eq!(s.retained_frames(), 1);
        assert_eq!(drops.get(), 0);
        close(&mut s);
        assert_eq!(results(&mut s), [vec![7]]);
        assert_eq!(drops.get(), 1);
        assert_eq!(budget.available_capacity(), 1);
    }

    #[test]
    fn pending_write_keeps_ownership_and_applies_backpressure() {
        let budget = FrameBudget::new(1);
        let mut config = config(1, 8, 2, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        let file_a = File::new(2);
        file_a.writing.close();
        let drops = Rc::new(Cell::new(0));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        file(&mut s, &mut cx, file_a.clone());
        frame(&mut s, &mut cx, 1, &drops);
        assert!(Pin::new(&mut s).poll_frame_ready(&mut cx).is_pending());
        assert_eq!(s.retained_frames(), 1);
        assert_eq!(drops.get(), 0);
        file_a.writing.open();
        assert!(matches!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(drops.get(), 1);
        Pin::new(&mut s)
            .enqueue_frame(Frame::new(2, &drops))
            .unwrap();
        close(&mut s);
        assert_eq!(results(&mut s), [vec![1, 2]]);
        assert_eq!(drops.get(), 2);
    }

    #[test]
    fn ready_frame_survives_progress_finalization_and_window_reduction() {
        for retries in [0, 1] {
            let budget = FrameBudget::new(8);
            let mut config = config(8, 16, 2, retries);
            let mut s = Scheduler::new(&budget, &mut config);
            let file_a = File::new(1);
            file_a.finalizing.close();
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let drops = Rc::new(Cell::new(0));
            file(&mut s, &mut cx, file_a.clone());
            file(&mut s, &mut cx, File::new(2));
            frame(&mut s, &mut cx, 1, &drops);
            assert!(matches!(
                Pin::new(&mut s).poll_frame_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            file_a.finalizing.open();
            s.poll_progress(&mut cx);
            s.set_window(Window::new(1, 1, 1).unwrap()).unwrap();
            assert!(s.granted_frames() >= 1);
            assert_eq!(
                Pin::new(&mut s).poll_next(&mut cx),
                Poll::Ready(Some(Ok(vec![1])))
            );
            assert!(matches!(
                Pin::new(&mut s).poll_frame_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            Pin::new(&mut s)
                .enqueue_frame(Frame::new(2, &drops))
                .unwrap();
            close(&mut s);
            assert_eq!(results(&mut s), [vec![2]]);
            assert_eq!(drops.get(), 2);
        }
    }

    #[test]
    fn ready_file_survives_window_reduction() {
        let budget = FrameBudget::new(4);
        let mut config = config(4, 8, 2, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        file(&mut s, &mut cx, File::new(2));
        assert!(matches!(
            Pin::new(&mut s).poll_file_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        s.set_window(Window::new(1, 1, 1).unwrap()).unwrap();
        s.poll_progress(&mut cx);
        Pin::new(&mut s).enqueue_file(File::new(2)).unwrap();
        assert_eq!(s.active, 2);
        assert!(Pin::new(&mut s).poll_file_ready(&mut cx).is_pending());
        close(&mut s);
        assert!(results(&mut s).is_empty());
    }

    #[test]
    fn variable_capacity_files_can_extend_beyond_the_horizon() {
        let budget = FrameBudget::new(4);
        let mut config = config(2, 4, 4, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        file(&mut s, &mut cx, File::new(3));
        file(&mut s, &mut cx, File::new(5));
        assert_eq!(s.horizon, 8);
        assert!(Pin::new(&mut s).poll_file_ready(&mut cx).is_pending());
        let drops = Rc::new(Cell::new(0));
        for value in 0..3 {
            frame(&mut s, &mut cx, value, &drops);
        }
        assert_eq!(
            Pin::new(&mut s).poll_next(&mut cx),
            Poll::Ready(Some(Ok(vec![0, 1, 2])))
        );
        assert!(Pin::new(&mut s).poll_file_ready(&mut cx).is_pending());
        for value in 3..8 {
            frame(&mut s, &mut cx, value, &drops);
        }
        assert_eq!(
            Pin::new(&mut s).poll_next(&mut cx),
            Poll::Ready(Some(Ok(vec![3, 4, 5, 6, 7])))
        );
        assert!(matches!(
            Pin::new(&mut s).poll_file_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        close(&mut s);
        assert!(results(&mut s).is_empty());
        assert_eq!(drops.get(), 8);
    }

    #[test]
    fn active_file_limit_waits_for_finalization() {
        let budget = FrameBudget::new(4);
        let mut config = config(4, 16, 1, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        let file_a = File::new(1);
        file_a.finalizing.close();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let drops = Rc::new(Cell::new(0));
        file(&mut s, &mut cx, file_a.clone());
        frame(&mut s, &mut cx, 1, &drops);
        assert!(Pin::new(&mut s).poll_file_ready(&mut cx).is_pending());
        file_a.finalizing.open();
        assert!(matches!(
            Pin::new(&mut s).poll_file_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        Pin::new(&mut s).enqueue_file(File::new(1)).unwrap();
        close(&mut s);
        assert_eq!(results(&mut s), [vec![1]]);
    }

    #[test]
    fn concurrent_finalization_still_emits_results_in_order() {
        for retries in [0, 1] {
            let budget = FrameBudget::new(8);
            let mut config = config(8, 16, 2, retries);
            let mut s = Scheduler::new(&budget, &mut config);
            let file_a = File::new(2);
            file_a.finalizing.close();
            let file_b = File::new(3);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let drops = Rc::new(Cell::new(0));
            file(&mut s, &mut cx, file_a.clone());
            file(&mut s, &mut cx, file_b.clone());
            for value in 0..5 {
                frame(&mut s, &mut cx, value, &drops);
            }
            close(&mut s);
            s.poll_progress(&mut cx);
            assert_eq!(file_b.finalized.get(), 1);
            assert_eq!(file_a.finalized.get(), 0);
            assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
            file_a.finalizing.open();
            assert_eq!(results(&mut s), [vec![0, 1], vec![2, 3, 4]]);
            assert_eq!(drops.get(), 5);
        }
    }

    #[test]
    fn replay_retains_frames_and_reserves_the_entire_destination() {
        let budget = FrameBudget::new(4);
        let mut config = config(1, 8, 2, 1);
        let mut s = Scheduler::new(&budget, &mut config);
        let file_a = File::new(4);
        file_a.finalizing.close();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let drops = Rc::new(Cell::new(0));
        file(&mut s, &mut cx, file_a.clone());
        assert!(matches!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(s.granted_frames(), 4);
        assert_eq!(budget.available_capacity(), 0);
        Pin::new(&mut s)
            .enqueue_frame(Frame::new(9, &drops))
            .unwrap();
        s.poll_progress(&mut cx);
        assert_eq!(drops.get(), 0);
        assert_eq!(s.retained_frames(), 1);
        close(&mut s);
        s.poll_progress(&mut cx);
        assert_eq!(drops.get(), 0);
        file_a.finalizing.open();
        assert_eq!(results(&mut s), [vec![9]]);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn retries_replay_without_clone_for_open_write_and_finalize_errors() {
        for stage in 0..3 {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 1, 1);
            let mut s = Scheduler::new(&budget, &mut config);
            let file_a = File::new(3);
            match stage {
                0 => file_a.open_failures.set(1),
                1 => file_a.write_failures.set(1),
                _ => file_a.finalize_failures.set(1),
            }
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let drops = Rc::new(Cell::new(0));
            file(&mut s, &mut cx, file_a.clone());
            for value in 0..3 {
                frame(&mut s, &mut cx, value, &drops);
            }
            close(&mut s);
            assert_eq!(results(&mut s), [vec![0, 1, 2]]);
            assert_eq!(file_a.opens.get(), 2);
            assert_eq!(drops.get(), 3);
            assert_eq!(budget.available_capacity(), 4);
        }
    }

    #[test]
    fn closing_empty_inputs_in_either_order_is_idempotent() {
        for frames_first in [false, true] {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 2, 1);
            let mut s = Scheduler::new(&budget, &mut config);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            if frames_first {
                Pin::new(&mut s).close_frames().unwrap();
            } else {
                Pin::new(&mut s).close_files().unwrap();
            }
            assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
            assert!(!s.is_terminated());
            close(&mut s);
            close(&mut s);
            assert_eq!(Pin::new(&mut s).poll_next(&mut cx), Poll::Ready(None));
            assert!(s.is_terminated());
        }
    }

    #[test]
    fn closing_partial_input_discards_unused_files_unopened() {
        for frames_first in [false, true] {
            for retries in [0, 1] {
                let budget = FrameBudget::new(8);
                let mut config = config(8, 16, 2, retries);
                let mut s = Scheduler::new(&budget, &mut config);
                let used = File::new(4);
                let unused = File::new(4);
                let waker = futures::task::noop_waker();
                let mut cx = Context::from_waker(&waker);
                let drops = Rc::new(Cell::new(0));
                file(&mut s, &mut cx, used.clone());
                file(&mut s, &mut cx, unused.clone());
                frame(&mut s, &mut cx, 5, &drops);
                if frames_first {
                    Pin::new(&mut s).close_frames().unwrap();
                    assert_eq!(
                        Pin::new(&mut s).poll_next(&mut cx),
                        Poll::Ready(Some(Ok(vec![5])))
                    );
                    assert!(Pin::new(&mut s).poll_next(&mut cx).is_pending());
                    Pin::new(&mut s).close_files().unwrap();
                    assert!(results(&mut s).is_empty());
                } else {
                    Pin::new(&mut s).close_files().unwrap();
                    Pin::new(&mut s).close_frames().unwrap();
                    assert_eq!(results(&mut s), [vec![5]]);
                }
                assert_eq!(used.opens.get(), 1);
                assert_eq!(unused.opens.get(), 0);
                assert_eq!(drops.get(), 1);
                assert_eq!(budget.available_capacity(), 8);
            }
        }
    }

    #[test]
    fn closing_cancels_an_unused_frame_reservation() {
        for retries in [0, 1] {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 1, retries);
            let mut s = Scheduler::new(&budget, &mut config);
            let unused = File::new(4);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            file(&mut s, &mut cx, unused.clone());
            assert!(matches!(
                Pin::new(&mut s).poll_frame_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            assert_eq!(budget.available_capacity(), 0);
            close(&mut s);
            assert!(results(&mut s).is_empty());
            assert_eq!(unused.opens.get(), 0);
            assert_eq!(budget.available_capacity(), 4);
        }
    }

    #[test]
    fn terminal_errors_release_frames_and_are_reported_once() {
        for via_stream in [false, true] {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 1, 0);
            let mut s = Scheduler::new(&budget, &mut config);
            let failed = File::new(2);
            failed.write_failures.set(1);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let drops = Rc::new(Cell::new(0));
            file(&mut s, &mut cx, failed);
            frame(&mut s, &mut cx, 0, &drops);
            s.poll_progress(&mut cx);
            assert_eq!(drops.get(), 1);
            assert_eq!(budget.available_capacity(), 4);
            if via_stream {
                assert_eq!(
                    Pin::new(&mut s).poll_next(&mut cx),
                    Poll::Ready(Some(Err(SchedulerError::Backend(Failure::Write))))
                );
            } else {
                assert_eq!(
                    Pin::new(&mut s).poll_frame_ready(&mut cx),
                    Poll::Ready(Err(SchedulerError::Backend(Failure::Write)))
                );
            }
            assert_eq!(Pin::new(&mut s).poll_next(&mut cx), Poll::Ready(None));
            assert!(s.is_terminated());
        }
    }

    #[test]
    fn closed_files_without_a_destination_fail_frame_admission() {
        let budget = FrameBudget::new(4);
        let mut config = config(4, 8, 1, 0);
        let mut s = Scheduler::new(&budget, &mut config);
        Pin::new(&mut s).close_files().unwrap();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Err(SchedulerError::Contract(
                ContractError::MissingWriteFile
            )))
        );
        assert_eq!(Pin::new(&mut s).poll_next(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn invalid_file_capacities_are_terminal() {
        for capacity in [0, 5] {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 1, 1);
            let mut s = Scheduler::new(&budget, &mut config);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(matches!(
                Pin::new(&mut s).poll_file_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            let expected = if capacity == 0 {
                ContractError::ZeroFrameCapacity
            } else {
                ContractError::FrameCapacityExceedsBudget
            };
            assert_eq!(
                Pin::new(&mut s).enqueue_file(File::new(capacity)),
                Err(expected.into())
            );
            assert_eq!(Pin::new(&mut s).poll_next(&mut cx), Poll::Ready(None));
            assert_eq!(budget.available_capacity(), 4);
        }
    }

    #[test]
    fn dropping_the_scheduler_releases_owned_frames_and_reservations() {
        let budget = FrameBudget::new(4);
        let mut config = config(4, 8, 1, 1);
        let mut s = Scheduler::new(&budget, &mut config);
        let file_a = File::new(4);
        file_a.writing.close();
        let drops = Rc::new(Cell::new(0));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        file(&mut s, &mut cx, file_a);
        frame(&mut s, &mut cx, 0, &drops);
        assert!(matches!(
            Pin::new(&mut s).poll_frame_ready(&mut cx),
            Poll::Ready(Ok(()))
        ));
        drop(s);
        assert_eq!(drops.get(), 1);
        assert_eq!(budget.available_capacity(), 4);
    }

    #[test]
    fn repeated_ready_polls_reuse_the_same_frame_reservation() {
        for retries in [0, 1] {
            let budget = FrameBudget::new(4);
            let mut config = config(4, 8, 1, retries);
            let mut s = Scheduler::new(&budget, &mut config);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            file(&mut s, &mut cx, File::new(4));
            assert!(matches!(
                Pin::new(&mut s).poll_frame_ready(&mut cx),
                Poll::Ready(Ok(()))
            ));
            let granted = s.granted_frames();
            let reserved = s.reserved;
            for _ in 0..3 {
                assert!(matches!(
                    Pin::new(&mut s).poll_frame_ready(&mut cx),
                    Poll::Ready(Ok(()))
                ));
                assert_eq!(s.granted_frames(), granted);
                assert_eq!(s.reserved, reserved);
                assert_eq!(s.retained_frames(), 0);
            }
            close(&mut s);
            assert!(results(&mut s).is_empty());
            assert_eq!(budget.available_capacity(), 4);
        }
    }

    #[test]
    fn closing_cancels_pending_budget_waits() {
        let budget = FrameBudget::new(4);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut held = budget.permit();
        assert!(held.poll_grow(&mut cx, 4, 4).is_ready());
        let mut config = config(4, 8, 1, 1);
        let mut s = Scheduler::new(&budget, &mut config);
        file(&mut s, &mut cx, File::new(4));
        assert!(Pin::new(&mut s).poll_frame_ready(&mut cx).is_pending());
        close(&mut s);
        held.shrink_to(0);
        assert_eq!(budget.available_capacity(), 4);
        assert!(results(&mut s).is_empty());
    }

    #[test]
    fn initial_configuration_errors_are_reported_once() {
        for zero_budget in [false, true] {
            let budget = FrameBudget::new(if zero_budget { 0 } else { 4 });
            let window = Window::default().with_target_frames(if zero_budget { 4 } else { 0 });
            let mut config = SchedulerConfig::new(window, 0);
            let mut s = Scheduler::new(&budget, &mut config);
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            let expected = if zero_budget {
                ContractError::ZeroBudget
            } else {
                ContractError::InvalidWindow
            };
            assert_eq!(
                Pin::new(&mut s).poll_file_ready(&mut cx),
                Poll::Ready(Err(expected.into()))
            );
            assert_eq!(Pin::new(&mut s).poll_next(&mut cx), Poll::Ready(None));
        }
    }
}
