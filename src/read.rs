use crate::{
    ContractError, FrameBudget, FramePermit, MAX_POLL_OPS, ReadFile, SchedulerConfig,
    SchedulerError, Window,
};
use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

/// A rejected read-file admission. The scheduler returns the original file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushError {
    /// The active-file limit or discovery horizon is full. Poll and retry later.
    Full,
    /// Admission is closed or the scheduler has terminated.
    Closed,
    /// The file violates its admission contract.
    Contract(ContractError),
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("read file window is full"),
            Self::Closed => f.write_str("read file admission is closed"),
            Self::Contract(error) => error.fmt(f),
        }
    }
}

impl Error for PushError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Contract(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
enum State {
    Opening,
    Reading,
}

struct Slot<F> {
    file: F,
    start: usize,
    count: u32,
    received: u32,
    retries: u32,
    state: State,
    blocked: bool,
}

struct Cell<T> {
    frame: Option<T>,
    complete: bool,
}

/// Sequential frame output with concurrent files and ordered budget allocation.
///
/// `push_file` transfers a descriptor immediately or returns it on refusal.
/// `open_ahead_frames` permits opening ahead of the buffering window;
/// `target_frames` controls that contiguous window within the shared budget.
/// `max_active_files` bounds admitted files, including opening files and files
/// awaiting ordered consumption.
///
/// Both `poll_read` and `poll_progress` drive I/O. Output exchanges complete
/// buffers with the caller using `mem::swap`; heap-backed payloads are not copied.
/// The factory is called only for a reserved frame without a reusable buffer.
/// Pass `&mut factory` to borrow a caller-owned factory.
///
/// An empty, open input returns `Pending` without registering a waker or
/// requesting budget. Mutations do not wake a task: after `push_file`,
/// `close_files`, or `set_window`, explicitly poll again. Backend and budget
/// waits register the caller's waker directly. No task or wake queue is created.
/// Fatal errors drop I/O and release budget, then appear once through `poll_read`.
pub struct ReadScheduler<'a, T, F: ReadFile<T> + Unpin, A: FnMut() -> T> {
    make_frame: A,
    config: &'a mut SchedulerConfig,
    permit: FramePermit<'a>,
    files: VecDeque<Slot<F>>,
    frames: VecDeque<Cell<T>>,
    recycled: Vec<T>,
    emitted: usize,
    discovered: usize,
    cursor: usize,
    closed: bool,
    terminated: bool,
    error: Option<SchedulerError<F::Error>>,
}

impl<T, F: ReadFile<T> + Unpin, A: FnMut() -> T> fmt::Debug for ReadScheduler<'_, T, F, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadScheduler")
            .field("window", &self.config.window)
            .field("granted_frames", &self.permit.capacity())
            .field("active_files", &self.files.len())
            .field("authorized_frames", &self.used())
            .field("files_closed", &self.closed)
            .field("terminated", &self.terminated)
            .finish()
    }
}

impl<'a, T, F: ReadFile<T> + Unpin, A: FnMut() -> T> ReadScheduler<'a, T, F, A> {
    /// Borrow the shared budget and configuration, and supply a buffer factory.
    ///
    /// # Errors
    /// Returns `InvalidWindow` for a zero target or active-file limit, or
    /// `ZeroBudget` for a zero shared capacity. No backend operation is started.
    pub fn new(
        make_frame: A,
        budget: &'a FrameBudget,
        config: &'a mut SchedulerConfig,
    ) -> Result<Self, ContractError> {
        config.window.validate()?;
        if budget.total_capacity() == 0 {
            return Err(ContractError::ZeroBudget);
        }
        Ok(Self {
            make_frame,
            config,
            permit: budget.permit(),
            files: VecDeque::new(),
            frames: VecDeque::new(),
            recycled: Vec::new(),
            emitted: 0,
            discovered: 0,
            cursor: 0,
            closed: false,
            terminated: false,
            error: None,
        })
    }

    /// Admit without polling or waking. Explicitly poll after a successful push.
    ///
    /// # Errors
    /// Returns the original file with `Full`, `Closed`, or `ZeroFrameCount`.
    /// Refusals preserve the scheduler and any deferred backend error.
    pub fn push_file(&mut self, file: F) -> Result<(), (PushError, F)> {
        if self.closed || self.terminated {
            return Err((PushError::Closed, file));
        }
        if self.files.len() >= self.config.window.max_active_files
            || self.discovered.wrapping_sub(self.emitted) >= self.config.window.horizon()
        {
            return Err((PushError::Full, file));
        }
        let count = file.frame_count();
        if count == 0 {
            return Err((PushError::Contract(ContractError::ZeroFrameCount), file));
        }
        let Ok(length) = usize::try_from(count) else {
            return Err((
                PushError::Contract(ContractError::InvalidFramePosition),
                file,
            ));
        };
        if self
            .discovered
            .wrapping_sub(self.emitted)
            .checked_add(length)
            .is_none()
        {
            return Err((
                PushError::Contract(ContractError::InvalidFramePosition),
                file,
            ));
        }
        self.files.push_back(Slot {
            file,
            start: self.discovered,
            count,
            received: 0,
            retries: 0,
            state: State::Opening,
            blocked: false,
        });
        // Logical positions wrap together; all live distances fit the window.
        self.discovered = self.discovered.wrapping_add(length);
        Ok(())
    }

    /// End admission. Idempotent and does not wake: explicitly poll again.
    /// EOF is returned after all declared frames of admitted files are consumed.
    pub fn close_files(&mut self) {
        self.closed = true;
    }

    /// Replace the window, preserving already authorized buffers and files.
    /// Explicitly poll again to apply new admission and buffering limits.
    ///
    /// # Errors
    /// Returns `InvalidWindow` for a zero target or active-file limit.
    pub fn set_window(&mut self, window: Window) -> Result<(), ContractError> {
        window.validate()?;
        self.config.window = window;
        let keep = self
            .permit
            .capacity()
            .min(window.target_frames.max(self.used()));
        self.permit.shrink_to(keep);
        Ok(())
    }

    /// Current configuration.
    pub fn window(&self) -> Window {
        self.config.window
    }

    /// Current local grant, without locking the shared budget.
    pub fn granted_frames(&self) -> usize {
        self.permit.capacity()
    }

    /// Whether EOF or the final error has already been delivered.
    pub fn is_terminated(&self) -> bool {
        self.terminated && self.error.is_none()
    }

    /// Drive I/O and exchange the next complete frame with `output`.
    ///
    /// `Some(())` swaps the caller's old frame without cloning. Heap-backed
    /// frames such as `Vec<u8>` exchange ownership without copying payload bytes.
    /// The returned frame belongs to the caller and is outside the shared budget.
    /// `Pending`, EOF, and errors leave `output` unchanged. Empty open input
    /// returns `Pending` without registering a waker; push then explicitly repoll.
    /// Dropping a waiting future preserves all scheduler-owned operations.
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        output: &mut T,
    ) -> Poll<Result<Option<()>, SchedulerError<F::Error>>> {
        self.poll_progress(cx);
        if let Some(error) = self.error.take() {
            return Poll::Ready(Err(error));
        }
        if self.terminated {
            return Poll::Ready(Ok(None));
        }
        if !self.frames.front().is_some_and(|cell| cell.complete) {
            return Poll::Pending;
        }
        let Some(mut cell) = self.frames.pop_front() else {
            return Poll::Pending;
        };
        let Some(mut frame) = cell.frame.take() else {
            self.fail(ContractError::InvalidFramePosition.into());
            return Poll::Ready(self.error.take().map_or(Ok(None), Err));
        };
        std::mem::swap(output, &mut frame);
        self.emitted = self.emitted.wrapping_add(1);
        self.recycled.push(frame);
        self.retire();
        self.adjust_capacity();
        Poll::Ready(Ok(Some(())))
    }

    /// Advance openings and authorized reads, consuming no output.
    /// Processes at most `max(256, active_files)` backend polls per call, allowing
    /// every active file to register its waker even in a larger configured window.
    /// Pending operations register the caller's waker directly and are polled at
    /// most once in this call.
    /// Deferred errors remain available to the next `poll_read`.
    /// Empty open input does nothing and registers no waker.
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) {
        if self.terminated {
            return;
        }
        if self.files.is_empty() {
            if self.permit.capacity() > 0 {
                self.permit.shrink_to(0);
            }
            self.terminated = self.closed;
            return;
        }
        if let Err(error) = self.drive(cx) {
            self.fail(error);
        }
    }

    /// Drive without consuming output in `select!`. This future never completes.
    /// Empty input requires the caller to push descriptors and explicitly repoll.
    pub async fn progress(&mut self) {
        std::future::poll_fn(|cx| {
            self.poll_progress(cx);
            Poll::<()>::Pending
        })
        .await
    }

    fn used(&self) -> usize {
        self.frames.len()
    }

    fn adjust_capacity(&mut self) {
        let keep = self.config.window.target_frames.max(self.used());
        let excess = self.permit.capacity().saturating_sub(keep);
        if excess >= 32 || (excess > 0 && keep == self.config.window.target_frames) {
            self.permit.shrink_to(keep);
        }
    }

    fn authorize(&mut self) {
        let limit = self.permit.capacity().min(self.config.window.target_frames);
        let available = limit.saturating_sub(self.used());
        let remaining = self.discovered.wrapping_sub(self.emitted);
        let extra = available.min(remaining.saturating_sub(self.frames.len()));
        self.frames.extend((0..extra).map(|_| Cell {
            frame: None,
            complete: false,
        }));
    }

    fn drive(&mut self, cx: &mut Context<'_>) -> Result<(), SchedulerError<F::Error>> {
        let target = self
            .config
            .window
            .target_frames
            .min(self.permit.total_capacity());
        if self.permit.capacity() < target {
            let minimum = self
                .permit
                .capacity()
                .saturating_add(target.min(32))
                .min(target);
            // Keep the existing batched FIFO budget policy.
            let _ = self.permit.poll_grow(cx, minimum, target)?;
        }
        self.authorize();
        for file in &mut self.files {
            file.blocked = false;
        }
        let mut remaining = MAX_POLL_OPS.max(self.files.len());
        loop {
            let count = self.files.len();
            if count == 0 {
                self.permit.shrink_to(0);
                self.terminated = self.closed;
                return Ok(());
            }
            let mut progressed = false;
            for _ in 0..count {
                let index = self.cursor % count;
                self.cursor = index.wrapping_add(1) % count;
                if self.poll_file(index, cx, &mut remaining)? {
                    progressed = true;
                }
                if remaining == 0 {
                    if self.files.iter().any(|slot| !slot.blocked) {
                        cx.waker().wake_by_ref();
                    }
                    return Ok(());
                }
            }
            self.retire();
            if self.files.is_empty() {
                self.permit.shrink_to(0);
                self.terminated = self.closed;
                return Ok(());
            }
            self.authorize();
            if !progressed {
                return Ok(());
            }
        }
    }

    fn poll_file(
        &mut self,
        index: usize,
        cx: &mut Context<'_>,
        remaining: &mut usize,
    ) -> Result<bool, SchedulerError<F::Error>> {
        let Some(slot) = self.files.get_mut(index) else {
            return Err(ContractError::InvalidFramePosition.into());
        };
        if slot.blocked {
            return Ok(false);
        }
        match slot.state {
            State::Opening => {
                *remaining = remaining.saturating_sub(1);
                match Pin::new(&mut slot.file).poll_open(cx) {
                    Poll::Pending => slot.blocked = true,
                    Poll::Ready(Ok(())) => {
                        slot.state = State::Reading;
                        return Ok(true);
                    }
                    Poll::Ready(Err(error)) => {
                        if slot.retries == self.config.max_retries {
                            return Err(SchedulerError::Backend(error));
                        }
                        slot.retries = slot.retries.saturating_add(1);
                        return Ok(true);
                    }
                }
            }
            State::Reading => {
                if slot.received == slot.count {
                    slot.blocked = true;
                    return Ok(false);
                }
                let offset = slot
                    .start
                    .wrapping_add(slot.received as usize)
                    .wrapping_sub(self.emitted);
                let Some(cell) = self.frames.get_mut(offset) else {
                    return Ok(false);
                };
                if cell.complete {
                    return Err(ContractError::InvalidFramePosition.into());
                }
                let frame = cell.frame.get_or_insert_with(|| {
                    self.recycled.pop().unwrap_or_else(&mut self.make_frame)
                });
                *remaining = remaining.saturating_sub(1);
                match Pin::new(&mut slot.file).poll_read(cx, frame) {
                    Poll::Pending => slot.blocked = true,
                    Poll::Ready(Err(error)) => return Err(SchedulerError::Backend(error)),
                    Poll::Ready(Ok(Some(()))) => {
                        cell.complete = true;
                        slot.received = slot.received.saturating_add(1);
                        slot.blocked = slot.received == slot.count;
                        return Ok(true);
                    }
                    Poll::Ready(Ok(None)) => return Err(ContractError::UnexpectedEof.into()),
                }
            }
        }
        Ok(false)
    }

    fn retire(&mut self) {
        while self
            .files
            .front()
            .is_some_and(|slot| self.emitted == slot.start.wrapping_add(slot.count as usize))
        {
            self.files.pop_front();
            self.cursor = self.cursor.saturating_sub(1);
        }
    }

    fn fail(&mut self, error: SchedulerError<F::Error>) {
        self.files.clear();
        self.frames.clear();
        self.recycled.clear();
        self.permit.shrink_to(0);
        self.terminated = true;
        self.error = Some(error);
    }
}
