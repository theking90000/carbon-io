use crate::{
    ContractError, FrameBudget, FramePermit, MAX_POLL_OPS, SchedulerConfig, Window, WriteFile,
};
use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    pin::Pin,
    task::{Context, Poll, Waker},
};

/// A frame built incrementally from borrowed input.
///
/// `Pending` must register the waker and leave the input unconsumed. Each
/// successful fill reports its consumption through `Output`. The caller decides
/// when input ends by polling the scheduler's `poll_close`.
/// Partial frames are movable; neither frames nor inputs need `Clone` or `Unpin`.
pub trait PartialFrame {
    /// Borrowed source, such as a byte slice or a reader with interior mutability.
    type Input: ?Sized;
    /// Result of one successful fill, such as a consumed byte count.
    type Output;
    /// Finished frame accepted by the file backend.
    type Frame;
    /// Input failure.
    type Error;
    /// Fill this frame without retaining the input reference.
    fn poll_fill(
        &mut self,
        cx: &Context<'_>,
        input: &Self::Input,
    ) -> Poll<Result<Self::Output, Self::Error>>;
    /// Whether this frame is ready to be sent to its destination.
    fn is_complete(&self) -> bool;
    /// Whether closing should discard this frame instead of sending it.
    fn is_empty(&self) -> bool;
    /// Finish a complete frame, or a nonempty partial frame during close.
    fn finish(self) -> Self::Frame;
}

/// Allocates empty, incomplete frames only after capacity has been reserved.
pub trait FrameAllocator {
    /// Incremental frame representation.
    type Partial: PartialFrame;
    /// Allocate one empty frame.
    fn allocate(&mut self) -> Self::Partial;
}

/// Builds owned byte frames of a fixed maximum size.
#[derive(Debug)]
pub struct BytesFrameAllocator {
    frame_size: usize,
}

impl BytesFrameAllocator {
    /// Reject a zero frame size.
    pub fn new(frame_size: usize) -> Result<Self, ContractError> {
        if frame_size == 0 {
            return Err(ContractError::InvalidPartialFrame);
        }
        Ok(Self { frame_size })
    }
}

/// Byte buffer being filled by [`BytesFrameAllocator`].
#[derive(Debug)]
pub struct BytesPartialFrame {
    bytes: Vec<u8>,
    frame_size: usize,
}

impl FrameAllocator for BytesFrameAllocator {
    type Partial = BytesPartialFrame;

    fn allocate(&mut self) -> Self::Partial {
        BytesPartialFrame {
            bytes: Vec::with_capacity(self.frame_size),
            frame_size: self.frame_size,
        }
    }
}

impl PartialFrame for BytesPartialFrame {
    type Input = [u8];
    type Output = usize;
    type Frame = Vec<u8>;
    type Error = std::convert::Infallible;

    fn poll_fill(&mut self, _: &Context<'_>, input: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let count = self
            .frame_size
            .saturating_sub(self.bytes.len())
            .min(input.len());
        self.bytes.extend(input.iter().take(count).copied());
        Poll::Ready(Ok(count))
    }

    fn is_complete(&self) -> bool {
        self.bytes.len() == self.frame_size
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Input, destination, or scheduler-contract failure.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteError<I, F> {
    /// Failed to fill a partial frame.
    Input(I),
    /// Failed to open, write, or close a destination after all retries.
    Backend(F),
    /// Invalid configuration or operation.
    Contract(ContractError),
}

impl<I: fmt::Display, F: fmt::Display> fmt::Display for WriteError<I, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(error) => write!(f, "input: {error}"),
            Self::Backend(error) => write!(f, "backend: {error}"),
            Self::Contract(error) => error.fmt(f),
        }
    }
}

impl<I: Error + 'static, F: Error + 'static> Error for WriteError<I, F> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Input(error) => error,
            Self::Backend(error) => error,
            Self::Contract(error) => error,
        })
    }
}

impl<I, F> From<ContractError> for WriteError<I, F> {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}

/// Consumption and FIFO maintenance reported by one write poll.
#[derive(Debug, PartialEq, Eq)]
pub struct WriteStatus<O> {
    /// `None` means no input was consumed; handle the file counts before polling again.
    pub output: Option<O>,
    /// Completed files at the head, available through `pop_swap`.
    pub completed_files: usize,
    /// Free destination slots, available through `push_file`.
    pub missing_files: usize,
}

/// A completed file and its close output, or no completed head yet.
pub type FileCloseResult<W> = Result<
    Option<(
        <W as AsyncFilesWrite>::File,
        <W as AsyncFilesWrite>::FileOutput,
    )>,
    <W as AsyncFilesWrite>::Error,
>;

/// Poll-driven file streaming with mandatory replacement while writing.
pub trait AsyncFilesWrite {
    /// Borrowed input used to fill a partial frame.
    type Input: ?Sized;
    /// Result of one fill.
    type Output;
    /// Unopened destination descriptor.
    type File;
    /// Successful file close result.
    type FileOutput;
    /// Input, backend, or contract error.
    type Error;
    /// Whether another unopened descriptor fits in the window.
    fn accept_file(&self) -> bool;
    /// Append an unopened descriptor to the FIFO.
    fn push_file(&mut self, file: Self::File) -> Result<(), Self::Error>;
    /// Drive I/O and fill at most one partial frame from borrowed input.
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
        input: &Self::Input,
    ) -> Poll<Result<WriteStatus<Self::Output>, Self::Error>>;
    /// Replace one completed head with an unopened tail, returning its descriptor and result.
    fn pop_swap(&mut self, file: Self::File)
    -> Result<(Self::File, Self::FileOutput), Self::Error>;
    /// Stop accepting input, flush the partial frame, and finalize all used files.
    /// A positive count requires `pop_file`; zero means closing is complete.
    fn poll_close(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<usize, Self::Error>>;
    /// Remove one completed head during close, without accepting a replacement.
    fn pop_file(&mut self) -> FileCloseResult<Self>;
}

enum FileState<R> {
    Idle,
    Opening,
    Writing,
    Closing,
    Closed(R),
}

// Streaming releases accepted frames; replay retains the complete attempt.
enum Frames<T> {
    Streaming(VecDeque<T>),
    Replay(Vec<T>),
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
            Self::Streaming(frames) => frames.push_back(frame),
            Self::Replay(frames) => frames.push(frame),
        }
    }

    fn next(&self, written: u32) -> Option<&T> {
        match self {
            Self::Streaming(frames) => frames.front(),
            Self::Replay(frames) => frames.get(written as usize),
        }
    }

    fn accepted(&mut self) -> usize {
        match self {
            Self::Streaming(frames) => usize::from(frames.pop_front().is_some()),
            Self::Replay(_) => 0,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Streaming(frames) => frames.len(),
            Self::Replay(frames) => frames.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::Streaming(frames) => frames.clear(),
            Self::Replay(frames) => frames.clear(),
        }
    }
}

type Partial<A> = <A as FrameAllocator>::Partial;
type Frame<A> = <Partial<A> as PartialFrame>::Frame;
/// Error type of a scheduler with allocator `A` and destination `F`.
pub type FilesWriteError<A, F> =
    WriteError<<Partial<A> as PartialFrame>::Error, <F as WriteFile<Frame<A>>>::Error>;
type ClosedSlot<A, F> = (Slot<Frame<A>, F>, <F as WriteFile<Frame<A>>>::Output);

struct Slot<T, F: WriteFile<T> + Unpin> {
    file: F,
    state: FileState<F::Output>,
    frames: Frames<T>,
    capacity: u32,
    assigned: u32,
    written: u32,
    retries: u32,
}

impl<T, F: WriteFile<T> + Unpin> Slot<T, F> {
    fn new(file: F, replay: bool) -> Self {
        let capacity = file.frame_capacity();
        Self {
            file,
            state: FileState::Idle,
            frames: Frames::new(replay),
            capacity,
            assigned: 0,
            written: 0,
            retries: 0,
        }
    }

    fn done(&self) -> bool {
        matches!(self.state, FileState::Closed(_))
    }

    // One state transition or backend poll, plus the capacity it releases.
    fn poll(
        &mut self,
        cx: &Context<'_>,
        closing: bool,
        max_retries: u32,
    ) -> Result<(bool, usize), F::Error> {
        let error = match self.state {
            FileState::Opening => match Pin::new(&mut self.file).poll_open(cx) {
                Poll::Pending => return Ok((false, 0)),
                Poll::Ready(Ok(())) => {
                    self.state = FileState::Writing;
                    return Ok((true, 0));
                }
                Poll::Ready(Err(error)) => error,
            },
            FileState::Writing => {
                if let Some(frame) = self.frames.next(self.written) {
                    match Pin::new(&mut self.file).poll_write(cx, frame) {
                        Poll::Pending => return Ok((false, 0)),
                        Poll::Ready(Ok(())) => {
                            self.written += 1;
                            return Ok((true, self.frames.accepted()));
                        }
                        Poll::Ready(Err(error)) => error,
                    }
                } else if self.assigned == self.capacity || closing {
                    self.state = FileState::Closing;
                    return Ok((true, 0));
                } else {
                    return Ok((false, 0));
                }
            }
            FileState::Closing => match Pin::new(&mut self.file).poll_close(cx) {
                Poll::Pending => return Ok((false, 0)),
                Poll::Ready(Ok(output)) => {
                    self.state = FileState::Closed(output);
                    self.frames.clear();
                    let released = if max_retries > 0 {
                        self.capacity as usize
                    } else {
                        0
                    };
                    return Ok((true, released));
                }
                Poll::Ready(Err(error)) => error,
            },
            FileState::Idle | FileState::Closed(_) => return Ok((false, 0)),
        };
        if self.retries == max_retries {
            return Err(error);
        }
        self.retries += 1;
        self.written = 0;
        self.state = FileState::Opening;
        Ok((true, 0))
    }
}

#[derive(PartialEq, Eq)]
enum Phase {
    Writing,
    Closing,
    Failed,
}

/// FIFO destinations and a single partial frame, driven only by write/close polls.
///
/// Fill the initial window with `while writer.accept_file() { writer.push_file(file)?; }`.
/// `poll_write` consumes borrowed input and reports completed heads and missing
/// descriptors. Replace every completed head with `pop_swap(file)`. `poll_close`
/// stops input permanently, sends a nonempty partial frame, and drops unused
/// descriptors without opening them. During close, remove completed heads with
/// `pop_file` until `poll_close` returns zero.
///
/// The allocator, shared budget, and configuration belong to the caller. Only
/// budget growth and release access shared state; backend polling uses the task's
/// waker directly, with no ready queue, additional locks, or spawned tasks.
/// Retries reserve an entire destination before allocating its first frame;
/// without retries, each successful backend write releases one frame immediately.
/// I/O errors cancel remaining work. Rejected API calls leave accepted work intact.
pub struct WriteScheduler<'a, A: FrameAllocator, F: WriteFile<Frame<A>> + Unpin> {
    allocator: &'a mut A,
    config: &'a mut SchedulerConfig,
    files: VecDeque<Slot<Frame<A>, F>>,
    partial: Option<Partial<A>>,
    permit: FramePermit<'a>,
    used: usize,
    fill_index: usize,
    poll_cursor: usize,
    phase: Phase,
    waker: Option<Waker>,
}

// Files are movable or supplied through a forwarding pinned pointer.
impl<A: FrameAllocator, F: WriteFile<Frame<A>> + Unpin> Unpin for WriteScheduler<'_, A, F> {}

impl<'a, A: FrameAllocator, F: WriteFile<Frame<A>> + Unpin> WriteScheduler<'a, A, F> {
    /// Borrow the allocator, shared frame budget, and configuration.
    pub fn new(
        allocator: &'a mut A,
        budget: &'a FrameBudget,
        config: &'a mut SchedulerConfig,
    ) -> Result<Self, ContractError> {
        config.window.validate()?;
        if budget.total_capacity() == 0 {
            return Err(ContractError::ZeroBudget);
        }
        Ok(Self {
            allocator,
            config,
            files: VecDeque::new(),
            partial: None,
            permit: budget.permit(),
            used: 0,
            fill_index: 0,
            poll_cursor: 0,
            phase: Phase::Writing,
            waker: None,
        })
    }

    /// Change the window without discarding accepted descriptors or frames.
    /// Writes use `target_frames` and `max_active_files`; discovery is descriptor-only.
    pub fn set_window(&mut self, window: Window) -> Result<(), ContractError> {
        window.validate()?;
        self.config.window = window;
        self.permit.shrink_to(
            self.permit
                .capacity()
                .min(window.target_frames.max(self.used)),
        );
        self.wake();
        Ok(())
    }

    /// Current window.
    pub fn window(&self) -> Window {
        self.config.window
    }

    /// Retained complete frames plus the current partial frame.
    pub fn retained_frames(&self) -> usize {
        self.files
            .iter()
            .map(|slot| slot.frames.len())
            .sum::<usize>()
            + usize::from(self.partial.is_some())
    }

    /// Local capacity occupied or reserved for replay.
    pub fn reserved_frames(&self) -> usize {
        self.used
    }

    /// Current grant from the shared frame budget.
    pub fn granted_frames(&self) -> usize {
        self.permit.capacity()
    }

    fn validate_file(&self, file: &F) -> Result<(), FilesWriteError<A, F>> {
        let capacity = file.frame_capacity();
        if capacity == 0 {
            return Err(ContractError::ZeroFrameCapacity.into());
        }
        if self.config.max_retries > 0 && capacity as usize > self.permit.total_capacity() {
            return Err(ContractError::FrameCapacityExceedsBudget.into());
        }
        Ok(())
    }

    fn status(
        &self,
        output: Option<<Partial<A> as PartialFrame>::Output>,
    ) -> WriteStatus<<Partial<A> as PartialFrame>::Output> {
        WriteStatus {
            output,
            completed_files: self.files.iter().take_while(|slot| slot.done()).count(),
            missing_files: self
                .config
                .window
                .max_active_files
                .saturating_sub(self.files.len()),
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn wait(&mut self, cx: &Context<'_>) {
        self.waker = Some(cx.waker().clone());
    }

    fn fail(&mut self) {
        self.phase = Phase::Failed;
        self.files.clear();
        self.partial = None;
        self.used = 0;
        self.permit.shrink_to(0);
        self.wake();
    }

    fn drive(&mut self, cx: &Context<'_>) -> Result<(), FilesWriteError<A, F>> {
        let count = self.files.len();
        if count == 0 {
            return Ok(());
        }
        let mut remaining = MAX_POLL_OPS;
        loop {
            let mut progressed = false;
            for _ in 0..count {
                let index = self.poll_cursor % count;
                self.poll_cursor = (index + 1) % count;
                let Some(slot) = self.files.get_mut(index) else {
                    return Err(ContractError::MissingWriteFile.into());
                };
                match slot.poll(cx, self.phase == Phase::Closing, self.config.max_retries) {
                    Ok((changed, released)) => {
                        self.used = self.used.saturating_sub(released);
                        if changed && slot.done() {
                            self.permit.shrink_to(self.used);
                        } else if released > 0 {
                            let desired = if self.phase == Phase::Closing {
                                self.used
                            } else {
                                self.config.window.target_frames.max(self.used)
                            };
                            self.permit.shrink_to(self.permit.capacity().min(desired));
                        }
                        if changed {
                            progressed = true;
                            remaining -= 1;
                            if remaining == 0 {
                                cx.waker().wake_by_ref();
                                return Ok(());
                            }
                        }
                    }
                    Err(error) => {
                        self.fail();
                        return Err(WriteError::Backend(error));
                    }
                }
            }
            if !progressed {
                return Ok(());
            }
        }
    }

    fn flush_partial(&mut self) -> Result<(), FilesWriteError<A, F>> {
        let Some(partial) = self.partial.take() else {
            return Ok(());
        };
        let Some(slot) = self.files.get_mut(self.fill_index) else {
            return Err(ContractError::MissingWriteFile.into());
        };
        if partial.is_empty() {
            let released = if self.config.max_retries == 0 {
                1
            } else if slot.assigned == 0 {
                slot.capacity as usize
            } else {
                0
            };
            self.used = self.used.saturating_sub(released);
            return Ok(());
        }
        slot.frames.push(partial.finish());
        slot.assigned += 1;
        if slot.assigned == 1 {
            slot.state = FileState::Opening;
        }
        if slot.assigned == slot.capacity {
            self.fill_index += 1;
        }
        Ok(())
    }

    fn ensure_capacity(
        &mut self,
        cx: &Context<'_>,
        minimum: usize,
    ) -> Result<bool, FilesWriteError<A, F>> {
        if self.permit.capacity() >= minimum {
            return Ok(true);
        }
        // Return unused fragments before waiting for a complete replay reservation.
        if self.permit.capacity() > self.used {
            self.permit.shrink_to(self.used);
        }
        let desired = self
            .config
            .window
            .target_frames
            .max(minimum)
            .min(self.permit.total_capacity());
        let mut budget_cx = Context::from_waker(cx.waker());
        match self.permit.poll_grow(&mut budget_cx, minimum, desired) {
            Poll::Ready(Ok(())) => Ok(true),
            Poll::Ready(Err(error)) => Err(error.into()),
            Poll::Pending => Ok(false),
        }
    }

    fn take_head(&mut self) -> Option<ClosedSlot<A, F>> {
        if !self.files.front()?.done() {
            return None;
        }
        let mut slot = self.files.pop_front()?;
        let FileState::Closed(output) = std::mem::replace(&mut slot.state, FileState::Idle) else {
            return None;
        };
        self.fill_index = self.fill_index.saturating_sub(1);
        self.poll_cursor = self.poll_cursor.saturating_sub(1);
        Some((slot, output))
    }
}

impl<A: FrameAllocator, F: WriteFile<Frame<A>> + Unpin> AsyncFilesWrite
    for WriteScheduler<'_, A, F>
{
    type Input = <Partial<A> as PartialFrame>::Input;
    type Output = <Partial<A> as PartialFrame>::Output;
    type File = F;
    type FileOutput = F::Output;
    type Error = FilesWriteError<A, F>;

    fn accept_file(&self) -> bool {
        self.phase == Phase::Writing && self.files.len() < self.config.window.max_active_files
    }

    fn push_file(&mut self, file: F) -> Result<(), Self::Error> {
        if self.phase != Phase::Writing {
            return Err(ContractError::InputClosed.into());
        }
        if !self.accept_file() {
            return Err(ContractError::AdmissionNotReady.into());
        }
        self.validate_file(&file)?;
        self.files
            .push_back(Slot::new(file, self.config.max_retries > 0));
        self.wake();
        Ok(())
    }

    fn poll_write(
        self: Pin<&mut Self>,
        cx: &Context<'_>,
        input: &Self::Input,
    ) -> Poll<Result<WriteStatus<Self::Output>, Self::Error>> {
        let this = self.get_mut();
        this.waker = None;
        if this.phase != Phase::Writing {
            return Poll::Ready(Err(ContractError::InputClosed.into()));
        }
        this.drive(cx)?;
        if this.partial.is_none() {
            if let Some(slot) = this.files.get(this.fill_index) {
                let needed = if this.config.max_retries == 0 {
                    1
                } else if slot.assigned == 0 {
                    slot.capacity as usize
                } else {
                    0
                };
                let limit = if this.config.max_retries == 0 {
                    this.permit
                        .total_capacity()
                        .min(this.config.window.target_frames)
                } else {
                    this.permit.total_capacity()
                };
                if needed <= limit.saturating_sub(this.used)
                    && this.ensure_capacity(cx, this.used + needed)?
                {
                    this.used += needed;
                    let partial = this.allocator.allocate();
                    if !partial.is_empty() || partial.is_complete() {
                        this.fail();
                        return Poll::Ready(Err(ContractError::InvalidPartialFrame.into()));
                    }
                    this.partial = Some(partial);
                }
            }
        }
        if let Some(partial) = &mut this.partial {
            match partial.poll_fill(cx, input) {
                Poll::Ready(Err(error)) => {
                    this.fail();
                    return Poll::Ready(Err(WriteError::Input(error)));
                }
                Poll::Ready(Ok(output)) => {
                    if partial.is_complete() {
                        if partial.is_empty() {
                            this.fail();
                            return Poll::Ready(Err(ContractError::InvalidPartialFrame.into()));
                        }
                        this.flush_partial()?;
                    }
                    // Return consumption before polling fallible backend work again.
                    return Poll::Ready(Ok(this.status(Some(output))));
                }
                Poll::Pending => {}
            }
        }
        let status = this.status(None);
        if status.completed_files > 0 || status.missing_files > 0 {
            Poll::Ready(Ok(status))
        } else {
            this.wait(cx);
            Poll::Pending
        }
    }

    fn pop_swap(&mut self, file: F) -> Result<(F, F::Output), Self::Error> {
        if self.phase != Phase::Writing {
            return Err(ContractError::InputClosed.into());
        }
        self.validate_file(&file)?;
        if !self.files.front().is_some_and(|slot| slot.done()) {
            return Err(ContractError::AdmissionNotReady.into());
        }
        let (mut slot, output) = self.take_head().ok_or(ContractError::AdmissionNotReady)?;
        let previous = std::mem::replace(&mut slot.file, file);
        slot.capacity = slot.file.frame_capacity();
        slot.assigned = 0;
        slot.written = 0;
        slot.retries = 0;
        slot.state = FileState::Idle;
        // ponytail: reuse the slot and replay buffer at every file boundary.
        self.files.push_back(slot);
        self.wake();
        Ok((previous, output))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<usize, Self::Error>> {
        let this = self.get_mut();
        this.waker = None;
        if this.phase == Phase::Failed {
            return Poll::Ready(Err(ContractError::InputClosed.into()));
        }
        if this.phase == Phase::Writing {
            this.phase = Phase::Closing;
            this.flush_partial()?;
            while this.files.back().is_some_and(|slot| slot.assigned == 0) {
                this.files.pop_back();
            }
            this.permit.shrink_to(this.used);
        }
        this.drive(cx)?;
        let completed = this.files.iter().take_while(|slot| slot.done()).count();
        if completed > 0 || this.files.is_empty() {
            Poll::Ready(Ok(completed))
        } else {
            this.wait(cx);
            Poll::Pending
        }
    }

    fn pop_file(&mut self) -> Result<Option<(F, F::Output)>, Self::Error> {
        if self.phase != Phase::Closing {
            return Err(ContractError::InputClosed.into());
        }
        let result = self.take_head().map(|(slot, output)| (slot.file, output));
        if result.is_some() {
            self.wake();
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;
    use std::{
        cell::{Cell, RefCell},
        marker::PhantomPinned,
        rc::Rc,
    };

    #[derive(Clone)]
    struct File {
        capacity: u32,
        opens: Rc<Cell<usize>>,
        blocked: Rc<Cell<bool>>,
        waker: Rc<RefCell<Option<Waker>>>,
        failures: Rc<Cell<u32>>,
        attempts: Rc<RefCell<Vec<Vec<Vec<u8>>>>>,
        frames: Vec<Vec<u8>>,
    }

    impl File {
        fn new(capacity: u32) -> Self {
            Self {
                capacity,
                opens: Rc::default(),
                blocked: Rc::default(),
                waker: Rc::default(),
                failures: Rc::default(),
                attempts: Rc::default(),
                frames: Vec::new(),
            }
        }

        fn unblock(&self) {
            self.blocked.set(false);
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    impl WriteFile<Vec<u8>> for File {
        type Error = &'static str;
        type Output = Vec<Vec<u8>>;

        fn frame_capacity(&self) -> u32 {
            self.capacity
        }

        fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            this.opens.set(this.opens.get() + 1);
            this.attempts.borrow_mut().push(Vec::new());
            this.frames.clear();
            Poll::Ready(Ok(()))
        }

        fn poll_write(
            self: Pin<&mut Self>,
            cx: &Context<'_>,
            frame: &Vec<u8>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.get_mut();
            if this.blocked.get() {
                *this.waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
            this.frames.push(frame.clone());
            if let Some(attempt) = this.attempts.borrow_mut().last_mut() {
                attempt.push(frame.clone());
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _: &Context<'_>,
        ) -> Poll<Result<Self::Output, Self::Error>> {
            let this = self.get_mut();
            if this.failures.get() > 0 {
                this.failures.set(this.failures.get() - 1);
                return Poll::Ready(Err("close"));
            }
            Poll::Ready(Ok(std::mem::take(&mut this.frames)))
        }
    }

    fn config(files: usize, retries: u32) -> SchedulerConfig {
        SchedulerConfig::new(Window::new(8, 8, files).unwrap(), retries)
    }

    struct PinnedFile {
        opens: Cell<usize>,
        writes: Cell<usize>,
        closes: Cell<usize>,
        _pin: PhantomPinned,
    }

    impl WriteFile<Vec<u8>> for PinnedFile {
        type Error = std::convert::Infallible;
        type Output = usize;

        fn frame_capacity(&self) -> u32 {
            1
        }

        fn poll_open(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<(), Self::Error>> {
            let this = self.as_ref().get_ref();
            this.opens.set(this.opens.get() + 1);
            if this.opens.get() == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn poll_write(
            self: Pin<&mut Self>,
            _: &Context<'_>,
            _: &Vec<u8>,
        ) -> Poll<Result<(), Self::Error>> {
            let this = self.as_ref().get_ref();
            this.writes.set(this.writes.get() + 1);
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Result<usize, Self::Error>> {
            let this = self.as_ref().get_ref();
            this.closes.set(this.closes.get() + 1);
            if this.closes.get() == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(this.writes.get()))
            }
        }
    }

    #[test]
    fn one_pinned_file_survives_every_state_and_pending_poll() {
        let file = Box::pin(PinnedFile {
            opens: Cell::new(0),
            writes: Cell::new(0),
            closes: Cell::new(0),
            _pin: PhantomPinned,
        });
        let address = file.as_ref().get_ref() as *const PinnedFile;
        let mut slot = Slot::<Vec<u8>, _>::new(file, false);
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);

        assert!(matches!(slot.state, FileState::Idle));
        assert_eq!(slot.poll(&cx, false, 0), Ok((false, 0)));
        assert_eq!(slot.file.as_ref().get_ref().opens.get(), 0);
        slot.frames.push(vec![1]);
        slot.assigned = 1;
        slot.state = FileState::Opening;
        assert_eq!(slot.poll(&cx, false, 0), Ok((false, 0)));
        assert!(matches!(slot.state, FileState::Opening));
        assert_eq!(slot.poll(&cx, false, 0), Ok((true, 0)));
        assert!(matches!(slot.state, FileState::Writing));
        assert_eq!(slot.poll(&cx, false, 0), Ok((true, 1)));
        assert_eq!(slot.poll(&cx, false, 0), Ok((true, 0)));
        assert!(matches!(slot.state, FileState::Closing));
        assert_eq!(slot.poll(&cx, false, 0), Ok((false, 0)));
        assert!(matches!(slot.state, FileState::Closing));
        assert_eq!(slot.poll(&cx, false, 0), Ok((true, 0)));
        assert!(matches!(slot.state, FileState::Closed(1)));
        assert_eq!(slot.poll(&cx, false, 0), Ok((false, 0)));
        assert_eq!(slot.file.as_ref().get_ref().closes.get(), 2);
        assert_eq!(slot.file.as_ref().get_ref() as *const PinnedFile, address);
    }

    #[test]
    fn partial_frame_and_fifo_replacement_then_close() {
        let budget = FrameBudget::new(2);
        let mut allocator = BytesFrameAllocator::new(3).unwrap();
        let mut config = config(2, 0);
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        let first = File::new(1);
        let spare = File::new(1);
        writer.push_file(first.clone()).unwrap();
        writer.push_file(spare.clone()).unwrap();
        assert!(!writer.accept_file());
        assert_eq!(first.opens.get(), 0);
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"ab"),
            Poll::Ready(Ok(WriteStatus {
                output: Some(2),
                completed_files: 0,
                missing_files: 0
            }))
        );
        assert_eq!(first.opens.get(), 0);
        assert_eq!(writer.retained_frames(), 1);
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"cd"),
            Poll::Ready(Ok(WriteStatus {
                output: Some(1),
                completed_files: 0,
                missing_files: 0
            }))
        );
        // The next call drains the first file and starts a partial frame in the second.
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"d"),
            Poll::Ready(Ok(WriteStatus {
                output: Some(1),
                completed_files: 1,
                missing_files: 0
            }))
        );
        let replacement = File::new(1);
        let (_, output) = writer.pop_swap(replacement.clone()).unwrap();
        assert_eq!(output, [b"abc".to_vec()]);
        assert!(matches!(
            writer.pop_file(),
            Err(WriteError::Contract(ContractError::InputClosed))
        ));
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(1)));
        assert_eq!(writer.pop_file().unwrap().unwrap().1, [b"d".to_vec()]);
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(0)));
        assert_eq!(replacement.opens.get(), 0);
        assert_eq!(budget.available_capacity(), 2);
        assert!(!writer.accept_file());
        assert!(matches!(
            Pin::new(&mut writer).poll_write(&cx, b"x"),
            Poll::Ready(Err(WriteError::Contract(ContractError::InputClosed)))
        ));
    }

    #[test]
    fn later_files_finish_without_passing_a_blocked_head() {
        let budget = FrameBudget::new(2);
        let mut allocator = BytesFrameAllocator::new(1).unwrap();
        let mut config = config(2, 0);
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        let first = File::new(1);
        first.blocked.set(true);
        let second = File::new(1);
        writer.push_file(first.clone()).unwrap();
        writer.push_file(second.clone()).unwrap();
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        assert!(Pin::new(&mut writer).poll_write(&cx, b"a").is_ready());
        assert!(Pin::new(&mut writer).poll_write(&cx, b"b").is_ready());
        assert!(Pin::new(&mut writer).poll_close(&cx).is_pending());
        assert!(writer.pop_file().unwrap().is_none());
        assert_eq!(*second.attempts.borrow(), [vec![b"b".to_vec()]]);
        assert_eq!(writer.retained_frames(), 1);
        first.unblock();
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(2)));
        assert_eq!(writer.pop_file().unwrap().unwrap().1, [b"a".to_vec()]);
        assert_eq!(writer.pop_file().unwrap().unwrap().1, [b"b".to_vec()]);
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(0)));
    }

    #[test]
    fn retries_replay_the_file_and_share_the_budget() {
        let budget = FrameBudget::new(2);
        let mut other = budget.permit();
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        let mut budget_cx = Context::from_waker(&waker);
        assert_eq!(other.poll_grow(&mut budget_cx, 1, 1), Poll::Ready(Ok(())));
        let mut allocator = BytesFrameAllocator::new(2).unwrap();
        let mut config = config(1, 1);
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        let file = File::new(2);
        file.failures.set(1);
        writer.push_file(file.clone()).unwrap();
        assert!(Pin::new(&mut writer).poll_write(&cx, b"ab").is_pending());
        assert_eq!(writer.retained_frames(), 0);
        assert_eq!(file.opens.get(), 0);
        other.shrink_to(0);
        assert!(Pin::new(&mut writer).poll_write(&cx, b"ab").is_ready());
        assert_eq!(writer.reserved_frames(), 2);
        assert!(Pin::new(&mut writer).poll_write(&cx, b"cd").is_ready());
        assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(1)));
        let expected = vec![b"ab".to_vec(), b"cd".to_vec()];
        assert_eq!(
            *file.attempts.borrow(),
            [expected.clone(), expected.clone()]
        );
        assert_eq!(writer.pop_file().unwrap().unwrap().1, expected);
        assert_eq!(budget.available_capacity(), 2);
    }

    #[test]
    fn missing_files_and_invalid_replacement_preserve_the_head() {
        let budget = FrameBudget::new(1);
        let mut allocator = BytesFrameAllocator::new(1).unwrap();
        let mut config = config(1, 0);
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"a"),
            Poll::Ready(Ok(WriteStatus {
                output: None,
                completed_files: 0,
                missing_files: 1
            }))
        );
        writer.push_file(File::new(1)).unwrap();
        assert!(Pin::new(&mut writer).poll_write(&cx, b"a").is_ready());
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"b"),
            Poll::Ready(Ok(WriteStatus {
                output: None,
                completed_files: 1,
                missing_files: 0
            }))
        );
        assert!(matches!(
            writer.pop_swap(File::new(0)),
            Err(WriteError::Contract(ContractError::ZeroFrameCapacity))
        ));
        assert_eq!(writer.pop_swap(File::new(1)).unwrap().1, [b"a".to_vec()]);
    }

    #[test]
    fn closing_empty_input_never_opens_a_destination_and_drop_releases_partial_budget() {
        let budget = FrameBudget::new(1);
        let mut allocator = BytesFrameAllocator::new(4).unwrap();
        let mut config = config(1, 0);
        let file = File::new(4);
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        {
            let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
            writer.push_file(file.clone()).unwrap();
            assert!(Pin::new(&mut writer).poll_write(&cx, b"").is_ready());
            assert_eq!(Pin::new(&mut writer).poll_close(&cx), Poll::Ready(Ok(0)));
            assert_eq!(file.opens.get(), 0);
        }
        {
            let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
            writer.push_file(file.clone()).unwrap();
            assert!(Pin::new(&mut writer).poll_write(&cx, b"a").is_ready());
            assert_eq!(budget.available_capacity(), 0);
        }
        assert_eq!(budget.available_capacity(), 1);
        assert_eq!(file.opens.get(), 0);
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use futures::task::noop_waker;
    use std::marker::PhantomPinned;

    struct FailingPartial(PhantomPinned);
    struct Allocator;
    struct File;

    impl PartialFrame for FailingPartial {
        type Input = [u8];
        type Output = usize;
        type Frame = FailingPartial;
        type Error = &'static str;

        fn poll_fill(&mut self, _: &Context<'_>, _: &[u8]) -> Poll<Result<usize, Self::Error>> {
            Poll::Ready(Err("input"))
        }
        fn is_complete(&self) -> bool {
            false
        }
        fn is_empty(&self) -> bool {
            true
        }
        fn finish(self) -> Self::Frame {
            self
        }
    }

    impl FrameAllocator for Allocator {
        type Partial = FailingPartial;
        fn allocate(&mut self) -> Self::Partial {
            FailingPartial(PhantomPinned)
        }
    }

    impl WriteFile<FailingPartial> for File {
        type Error = &'static str;
        type Output = ();
        fn frame_capacity(&self) -> u32 {
            1
        }
        fn poll_open(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err("must stay unopened"))
        }
        fn poll_write(
            self: Pin<&mut Self>,
            _: &Context<'_>,
            _: &FailingPartial,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err("must stay unopened"))
        }
        fn poll_close(self: Pin<&mut Self>, _: &Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Err("must stay unopened"))
        }
    }

    #[test]
    fn input_failure_cancels_work_and_returns_the_budget_without_unpin_or_clone() {
        let budget = FrameBudget::new(1);
        let mut config = SchedulerConfig::default();
        let mut allocator = Allocator;
        let mut writer = WriteScheduler::new(&mut allocator, &budget, &mut config).unwrap();
        writer.push_file(File).unwrap();
        let waker = noop_waker();
        let cx = Context::from_waker(&waker);
        assert_eq!(
            Pin::new(&mut writer).poll_write(&cx, b"a"),
            Poll::Ready(Err(WriteError::Input("input")))
        );
        assert_eq!(budget.available_capacity(), 1);
        assert_eq!(writer.retained_frames(), 0);
        assert!(!writer.accept_file());
    }
}
