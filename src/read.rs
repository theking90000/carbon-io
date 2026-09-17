use crate::{
    ContractError, FrameBudget, FramePermit, MAX_POLL_OPS, ReadFile, SchedulerConfig,
    SchedulerError, Window, ready::ReadyQueue,
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
const BUDGET: usize = 1;
const FIRST_SLOT: usize = 2;

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
/// Polling drives all work. No background task is created. The input yields files
/// directly; errors come from each file's opening future and reader.
/// Input streams and backend futures may be `!Unpin`.
pub struct ReadScheduler<T, S>
where
    S: Stream,
    S::Item: ReadFile<T>,
{
    files: Option<Pin<Box<S>>>,
    slots: Vec<Slot<T, S::Item>>,
    order: VecDeque<usize>,
    free: Vec<usize>,
    ready: ReadyQueue,
    permit: FramePermit,
    config: SchedulerConfig,
    ring: Vec<Option<T>>,
    head: usize,
    emitted: usize,
    discovered: usize,
    authorized: usize,
    allow_cursor: usize,
    active: usize,
    error: Option<SchedulerError<<S::Item as ReadFile<T>>::Error>>,
    files_ready: bool,
    terminated: bool,
}

// Only boxed I/O is structurally pinned; frames and file descriptors are not.
impl<T, S> Unpin for ReadScheduler<T, S>
where
    S: Stream,
    S::Item: ReadFile<T>,
{
}

impl<T, S> ReadScheduler<T, S>
where
    S: Stream,
    S::Item: ReadFile<T>,
{
    /// Build a scheduler. Invalid initial configuration is returned by its stream.
    pub fn new(files: S, budget: FrameBudget, config: SchedulerConfig) -> Self {
        let mut ready = ReadyQueue::new();
        ready.add();
        ready.add();
        ready.schedule(FILES);
        ready.schedule(BUDGET);
        let error = config
            .window
            .validate()
            .err()
            .or_else(|| (budget.total_capacity() == 0).then_some(ContractError::ZeroBudget))
            .map(Into::into);
        Self {
            files: Some(Box::pin(files)),
            slots: Vec::new(),
            order: VecDeque::new(),
            free: Vec::new(),
            ready,
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
            files_ready: true,
            terminated: false,
        }
    }
    /// Change the window. Already authorized frames and opened files are retained.
    /// The caller must poll again to drive the updated configuration.
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
        self.schedule_files();
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

    fn schedule_files(&mut self) {
        if self.files_ready {
            self.ready.schedule(FILES);
        }
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
    fn discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as ReadFile<T>>::Error>> {
        if self.active >= self.config.window.max_active_files
            || self.discovered.wrapping_sub(self.emitted) >= self.config.window.horizon()
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
                return Ok(());
            }
            Poll::Ready(Some(file)) => file,
        };
        let count = file.frame_count();
        if count == 0 {
            return Err(ContractError::ZeroFrameCount.into());
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
            let signal_id = self.ready.add();
            debug_assert_eq!(signal_id, id + FIRST_SLOT);
            id
        };
        self.discovered = self.discovered.wrapping_add(count as usize);
        self.active += 1;
        self.order.push_back(id);
        self.ready.schedule(id + FIRST_SLOT);
        self.schedule_files();
        self.allow();
        Ok(())
    }
    fn poll_slot(
        &mut self,
        id: usize,
        cx: &mut Context<'_>,
    ) -> Result<(), SchedulerError<<S::Item as ReadFile<T>>::Error>> {
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
                    slot.io.set(ReadIo::Opening {
                        future: slot.file.as_ref().unwrap().open(),
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
                        self.schedule_files();
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
            self.schedule_files();
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
            self.schedule_files();
        }
    }
    fn finish(&mut self) {
        self.files = None;
        self.slots.clear();
        self.order.clear();
        self.ring.clear();
        self.permit.shrink_to(0);
        self.terminated = true;
    }
}

impl<T, S> Stream for ReadScheduler<T, S>
where
    S: Stream,
    S::Item: ReadFile<T>,
{
    type Item = Result<T, SchedulerError<<S::Item as ReadFile<T>>::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        if let Some(error) = this.error.take() {
            this.finish();
            return Poll::Ready(Some(Err(error)));
        }
        // Drain a previously filled window without locks, atomics, or slot scans.
        if let Some(frame) = this.take_frame() {
            return Poll::Ready(Some(Ok(frame)));
        }
        this.ready.register(cx.waker());
        for _ in 0..MAX_POLL_OPS {
            let Some(id) = this.ready.pop() else { break };
            let waker = this.ready.waker(id);
            let mut child = Context::from_waker(&waker);
            let result = match id {
                FILES => {
                    this.files_ready = true;
                    this.discover(&mut child)
                }
                BUDGET => {
                    this.grow(&mut child);
                    Ok(())
                }
                _ => this.poll_slot(id - FIRST_SLOT, &mut child),
            };
            if let Err(error) = result {
                this.finish();
                return Poll::Ready(Some(Err(error)));
            }
        }
        this.retire();
        if let Some(frame) = this.take_frame() {
            return Poll::Ready(Some(Ok(frame)));
        }
        if this.files.is_none() && this.order.is_empty() {
            this.finish();
            return Poll::Ready(None);
        }
        if this.ready.has_work() {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}
impl<T, S> FusedStream for ReadScheduler<T, S>
where
    S: Stream,
    S::Item: ReadFile<T>,
{
    fn is_terminated(&self) -> bool {
        self.terminated
    }
}
