#![allow(dead_code)]
use carbon_io::{
    AsyncFilesWrite, FrameAllocator, PartialFrame, ReadFile, SchedulerConfig, Window, WriteFile,
};
use futures::Stream;
use pin_project_lite::pin_project;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    marker::PhantomPinned,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
pub struct Counter(pub AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
pub fn context_waker() -> (Arc<Counter>, Waker) {
    let count = Arc::new(Counter::default());
    (count.clone(), Waker::from(count))
}
pub fn poll<S: Stream + Unpin>(s: &mut S) -> Poll<Option<S::Item>> {
    let (_, w) = context_waker();
    Pin::new(s).poll_next(&mut Context::from_waker(&w))
}
pub fn collect<S: Stream + Unpin>(mut s: S) -> Vec<S::Item> {
    let (counter, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let mut items = Vec::new();
    for _ in 0..100_000 {
        let before = counter.0.load(Ordering::Relaxed);
        match Pin::new(&mut s).poll_next(&mut cx) {
            Poll::Ready(Some(item)) => items.push(item),
            Poll::Ready(None) => return items,
            Poll::Pending => assert!(
                counter.0.load(Ordering::Relaxed) > before,
                "stalled without a wakeup"
            ),
        }
    }
    panic!("scheduler failed to terminate");
}
pub fn config(target: usize, ahead: usize, active: usize, retries: u32) -> SchedulerConfig {
    let mut window = Window::default();
    window.target_frames = target;
    window.open_ahead_frames = ahead;
    window.max_active_files = active;
    SchedulerConfig::new(window, retries)
}
#[derive(Clone)]
pub struct Gate(Rc<GateState>);
struct GateState {
    open: Cell<bool>,
    polls: Cell<usize>,
    waker: RefCell<Option<Waker>>,
}
impl Gate {
    pub fn new(open: bool) -> Self {
        Self(Rc::new(GateState {
            open: Cell::new(open),
            polls: Cell::new(0),
            waker: RefCell::new(None),
        }))
    }
    pub fn ready(&self, cx: &Context<'_>) -> bool {
        self.0.polls.set(self.polls() + 1);
        if self.0.open.get() {
            true
        } else {
            *self.0.waker.borrow_mut() = Some(cx.waker().clone());
            false
        }
    }
    pub fn open(&self) {
        self.0.open.set(true);
        if let Some(w) = self.0.waker.borrow_mut().take() {
            w.wake();
        }
    }
    pub fn close(&self) {
        self.0.open.set(false);
    }
    pub fn polls(&self) -> usize {
        self.0.polls.get()
    }
}
pub struct Read {
    pub count: u32,
    pub values: Vec<Result<usize, &'static str>>,
    pub opening: Gate,
    pub reading: Gate,
    pub opens: Rc<Cell<usize>>,
    pub open_failures: Rc<Cell<usize>>,
    pub drops: Rc<Cell<usize>>,
    reader: Option<Pin<Box<Reader>>>,
    opening_started: bool,
}
impl Read {
    pub fn new(values: impl IntoIterator<Item = usize>) -> Self {
        let values: Vec<_> = values.into_iter().map(Ok).collect();
        Self {
            count: values.len() as u32,
            values,
            opening: Gate::new(true),
            reading: Gate::new(true),
            opens: Rc::new(Cell::new(0)),
            open_failures: Rc::new(Cell::new(0)),
            drops: Rc::new(Cell::new(0)),
            reader: None,
            opening_started: false,
        }
    }
}
impl Clone for Read {
    fn clone(&self) -> Self {
        Self {
            count: self.count,
            values: self.values.clone(),
            opening: self.opening.clone(),
            reading: self.reading.clone(),
            opens: self.opens.clone(),
            open_failures: self.open_failures.clone(),
            drops: self.drops.clone(),
            reader: None,
            opening_started: false,
        }
    }
}
impl std::fmt::Debug for Read {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Read").field("count", &self.count).finish()
    }
}
pin_project! { pub struct Reader { values: VecDeque<Result<usize, &'static str>>, gate: Gate, drops: Rc<Cell<usize>>, #[pin] _pin: PhantomPinned }
    impl PinnedDrop for Reader { fn drop(this: Pin<&mut Self>) { this.drops.set(this.drops.get() + 1); } }
}
impl Stream for Reader {
    type Item = Result<usize, &'static str>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if !this.gate.ready(cx) {
            return Poll::Pending;
        }
        Poll::Ready(this.values.pop_front())
    }
}
impl ReadFile<usize> for Read {
    type Error = &'static str;
    fn frame_count(&self) -> u32 {
        self.count
    }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if !this.opening_started {
            this.opens.set(this.opens.get() + 1);
            this.opening_started = true;
        }
        if !this.opening.ready(cx) {
            return Poll::Pending;
        }
        if this.open_failures.get() > 0 {
            this.open_failures.set(this.open_failures.get() - 1);
            this.opening_started = false;
            return Poll::Ready(Err("open"));
        }
        this.reader = Some(Box::pin(Reader {
            values: this.values.clone().into(),
            gate: this.reading.clone(),
            drops: this.drops.clone(),
            _pin: PhantomPinned,
        }));
        Poll::Ready(Ok(()))
    }
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &mut usize,
    ) -> Poll<Result<Option<()>, Self::Error>> {
        let Some(reader) = self.get_mut().reader.as_mut() else {
            return Poll::Ready(Err("not open"));
        };
        match reader.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(value))) => {
                *frame = value;
                Poll::Ready(Ok(Some(())))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(None) => Poll::Ready(Ok(None)),
        }
    }
}
#[derive(Debug)]
pub struct Frame {
    pub value: usize,
    pub drops: Rc<Cell<usize>>,
}
impl Drop for Frame {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
#[derive(Clone)]
pub struct Write {
    attempt: Option<usize>,
    pub capacity: u32,
    pub opens: Rc<Cell<usize>>,
    pub opening: Gate,
    pub writing: Gate,
    pub finalizing: Gate,
    pub attempts: Rc<RefCell<Vec<Vec<usize>>>>,
    pub open_failures: Rc<Cell<usize>>,
    pub write_failures: Rc<Cell<usize>>,
    pub finalize_failures: Rc<Cell<usize>>,
    pub finalized: Rc<Cell<usize>>,
    pub drops: Rc<Cell<usize>>,
}
impl Write {
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity,
            attempt: None,
            opens: Rc::default(),
            opening: Gate::new(true),
            writing: Gate::new(true),
            finalizing: Gate::new(true),
            attempts: Rc::default(),
            open_failures: Rc::default(),
            write_failures: Rc::default(),
            finalize_failures: Rc::default(),
            finalized: Rc::default(),
            drops: Rc::default(),
        }
    }
}
impl WriteFile<Frame> for Write {
    type Error = &'static str;
    type Output = Vec<usize>;
    fn frame_capacity(&self) -> u32 {
        self.capacity
    }
    fn poll_open(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if !this.opening.ready(cx) {
            return Poll::Pending;
        }
        this.opens.set(this.opens.get() + 1);
        if this.open_failures.get() > 0 {
            this.open_failures.set(this.open_failures.get() - 1);
            return Poll::Ready(Err("open"));
        }
        this.attempt = Some(this.attempts.borrow().len());
        this.attempts.borrow_mut().push(Vec::new());
        Poll::Ready(Ok(()))
    }
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Frame,
    ) -> Poll<Result<(), Self::Error>> {
        if !self.writing.ready(cx) {
            return Poll::Pending;
        }
        let mut attempts = self.attempts.borrow_mut();
        let frames = &mut attempts[self.attempt.unwrap()];
        if !frames.is_empty() && self.write_failures.get() > 0 {
            self.write_failures.set(self.write_failures.get() - 1);
            return Poll::Ready(Err("write"));
        }
        frames.push(frame.value);
        Poll::Ready(Ok(()))
    }
    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        if !self.finalizing.ready(cx) {
            return Poll::Pending;
        }
        if self.finalize_failures.get() > 0 {
            self.finalize_failures.set(self.finalize_failures.get() - 1);
            return Poll::Ready(Err("finalize"));
        }
        self.finalized.set(self.finalized.get() + 1);
        Poll::Ready(Ok(self.attempts.borrow()[self.attempt.unwrap()].clone()))
    }
}
impl Drop for Write {
    fn drop(&mut self) {
        if self.attempt.is_some() {
            self.drops.set(self.drops.get() + 1);
        }
    }
}

pub struct Allocator {
    pub drops: Rc<Cell<usize>>,
    pub filling: Gate,
}
impl Default for Allocator {
    fn default() -> Self {
        Self {
            drops: Rc::default(),
            filling: Gate::new(true),
        }
    }
}
pub struct Partial {
    frame: Option<Frame>,
    drops: Rc<Cell<usize>>,
    filling: Gate,
}
impl FrameAllocator for Allocator {
    type Partial = Partial;
    fn allocate(&mut self) -> Partial {
        Partial {
            frame: None,
            drops: self.drops.clone(),
            filling: self.filling.clone(),
        }
    }
}
impl PartialFrame for Partial {
    type Input = usize;
    type Output = ();
    type Frame = Frame;
    type Error = std::convert::Infallible;
    fn poll_fill(&mut self, cx: &Context<'_>, value: &usize) -> Poll<Result<(), Self::Error>> {
        if !self.filling.ready(cx) {
            return Poll::Pending;
        }
        self.frame = Some(Frame {
            value: *value,
            drops: self.drops.clone(),
        });
        Poll::Ready(Ok(()))
    }
    fn is_complete(&self) -> bool {
        self.frame.is_some()
    }
    fn is_empty(&self) -> bool {
        self.frame.is_none()
    }
    fn finish(self) -> Frame {
        self.frame.unwrap()
    }
}
pub type Writer<'a> = carbon_io::WriteScheduler<'a, Allocator, Write>;
pub fn write_value(writer: &mut Writer<'_>, value: usize) {
    let (_, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let Poll::Ready(Ok(status)) = Pin::new(writer).poll_write(&mut cx, &value) else {
        panic!("value was not accepted");
    };
    assert_eq!(status.output, Some(()));
}
pub fn close_writer(writer: &mut Writer<'_>) -> Vec<Vec<usize>> {
    let (counter, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let mut outputs = Vec::new();
    for _ in 0..100_000 {
        let before = counter.0.load(Ordering::Relaxed);
        match Pin::new(&mut *writer).poll_close(&mut cx) {
            Poll::Ready(Ok(0)) => return outputs,
            Poll::Ready(Ok(count)) => {
                for _ in 0..count {
                    outputs.push(writer.pop_file().unwrap().unwrap().1);
                }
            }
            Poll::Ready(Err(error)) => panic!("{error:?}"),
            Poll::Pending => assert!(
                counter.0.load(Ordering::Relaxed) > before,
                "stalled without wakeup"
            ),
        }
    }
    panic!("writer failed to close");
}

pin_project! { pub struct PinnedStream<S> { #[pin] inner: S, #[pin] _pin: PhantomPinned } }
impl<S> PinnedStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            _pin: PhantomPinned,
        }
    }
}
impl<S: Stream> Stream for PinnedStream<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

/// Test-only adapter for caller-owned descriptor streams and complete-frame output.
pub struct ReadDriver<'a, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    scheduler: carbon_io::ReadScheduler<'a, T, S::Item, fn() -> T>,
    files: &'a mut S,
    pending: Option<S::Item>,
    closed: bool,
    output: T,
    error: Option<carbon_io::SchedulerError<<S::Item as ReadFile<T>>::Error>>,
}
impl<T, S> Unpin for ReadDriver<'_, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
}

impl<'a, T, S> ReadDriver<'a, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    pub fn new(
        files: &'a mut S,
        budget: &'a carbon_io::FrameBudget,
        config: &'a mut SchedulerConfig,
    ) -> Self {
        Self {
            scheduler: carbon_io::ReadScheduler::new(T::default as fn() -> T, budget, config)
                .unwrap(),
            files,
            pending: None,
            closed: false,
            output: T::default(),
            error: None,
        }
    }
    fn feed(&mut self, cx: &mut Context<'_>) -> bool {
        let mut changed = false;
        for _ in 0..64 {
            if self.closed {
                break;
            }
            let file = match self.pending.take() {
                Some(file) => file,
                None => match Pin::new(&mut *self.files).poll_next(cx) {
                    Poll::Pending => break,
                    Poll::Ready(None) => {
                        self.closed = true;
                        self.scheduler.close_files();
                        changed = true;
                        break;
                    }
                    Poll::Ready(Some(file)) => file,
                },
            };
            match self.scheduler.push_file(file) {
                Ok(()) => changed = true,
                Err((carbon_io::PushError::Full, file)) => {
                    self.pending = Some(file);
                    break;
                }
                Err((carbon_io::PushError::Contract(error), _)) => {
                    self.error = Some(error.into());
                    self.closed = true;
                    break;
                }
                Err((carbon_io::PushError::Closed, _)) => {
                    self.closed = true;
                    break;
                }
            }
        }
        changed
    }
    pub fn poll_progress(&mut self, cx: &mut Context<'_>) {
        self.feed(cx);
        self.scheduler.poll_progress(cx);
        if self.feed(cx) {
            cx.waker().wake_by_ref();
        }
    }
    pub async fn progress(&mut self) {
        std::future::poll_fn(|cx| {
            self.poll_progress(cx);
            Poll::<()>::Pending
        })
        .await
    }
}
impl<'a, T, S> std::ops::Deref for ReadDriver<'a, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    type Target = carbon_io::ReadScheduler<'a, T, S::Item, fn() -> T>;
    fn deref(&self) -> &Self::Target {
        &self.scheduler
    }
}
impl<T, S> std::ops::DerefMut for ReadDriver<'_, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scheduler
    }
}
impl<T, S> Stream for ReadDriver<'_, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    type Item = Result<T, carbon_io::SchedulerError<<S::Item as ReadFile<T>>::Error>>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.feed(cx);
        if let Some(error) = this.error.take() {
            return Poll::Ready(Some(Err(error)));
        }
        match this.scheduler.poll_read(cx, &mut this.output) {
            Poll::Ready(Ok(Some(()))) => Poll::Ready(Some(Ok(std::mem::take(&mut this.output)))),
            Poll::Ready(Ok(None)) => Poll::Ready(None),
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Pending => {
                if this.feed(cx) {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }
}
impl<T, S> futures::stream::FusedStream for ReadDriver<'_, T, S>
where
    T: Default,
    S: Stream + Unpin,
    S::Item: ReadFile<T> + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.error.is_none() && self.scheduler.is_terminated()
    }
}

pub fn admit_read_file<T, F, A>(scheduler: &mut carbon_io::ReadScheduler<'_, T, F, A>, file: F)
where
    F: ReadFile<T> + Unpin,
    A: FnMut() -> T,
{
    assert!(scheduler.push_file(file).is_ok());
}

pub fn poll_read<T, F, A>(
    scheduler: &mut carbon_io::ReadScheduler<'_, T, F, A>,
) -> Poll<Option<Result<T, carbon_io::SchedulerError<F::Error>>>>
where
    T: Default,
    F: ReadFile<T> + Unpin,
    A: FnMut() -> T,
{
    let (_, waker) = context_waker();
    let mut output = T::default();
    match scheduler.poll_read(&mut Context::from_waker(&waker), &mut output) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(Some(()))) => Poll::Ready(Some(Ok(output))),
        Poll::Ready(Ok(None)) => Poll::Ready(None),
        Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
    }
}

pub fn collect_read<T, F, A>(
    scheduler: &mut carbon_io::ReadScheduler<'_, T, F, A>,
) -> Vec<Result<T, carbon_io::SchedulerError<F::Error>>>
where
    T: Default,
    F: ReadFile<T> + Unpin,
    A: FnMut() -> T,
{
    let (counter, waker) = context_waker();
    let mut cx = Context::from_waker(&waker);
    let mut output = T::default();
    let mut values = Vec::new();
    for _ in 0..100_000 {
        let before = counter.0.load(Ordering::Relaxed);
        match scheduler.poll_read(&mut cx, &mut output) {
            Poll::Ready(Ok(Some(()))) => values.push(Ok(std::mem::take(&mut output))),
            Poll::Ready(Err(error)) => values.push(Err(error)),
            Poll::Ready(Ok(None)) => return values,
            Poll::Pending => assert!(
                counter.0.load(Ordering::Relaxed) > before,
                "stalled without wakeup"
            ),
        }
    }
    panic!("reader failed to finish");
}
