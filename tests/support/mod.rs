#![allow(dead_code)]
use carbon_io::{FrameWriter, ReadFile, SchedulerConfig, Window, WriteFile};
use futures::{Stream, stream};
use pin_project_lite::pin_project;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
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
    SchedulerConfig {
        window: Window {
            target_frames: target,
            open_ahead_frames: ahead,
            max_active_files: active,
        },
        max_retries: retries,
    }
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
#[derive(Clone)]
pub struct Read {
    pub count: u32,
    pub values: Vec<Result<usize, &'static str>>,
    pub opening: Gate,
    pub reading: Gate,
    pub opens: Rc<Cell<usize>>,
    pub open_failures: Rc<Cell<usize>>,
    pub drops: Rc<Cell<usize>>,
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
        }
    }
}
pin_project! { pub struct ReadOpen { file: Option<Read>, #[pin] _pin: PhantomPinned } }
impl Future for ReadOpen {
    type Output = Result<Reader, &'static str>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let file = this.file.as_ref().unwrap();
        if !file.opening.ready(cx) {
            return Poll::Pending;
        }
        if file.open_failures.get() > 0 {
            file.open_failures.set(file.open_failures.get() - 1);
            return Poll::Ready(Err("open"));
        }
        let file = this.file.take().unwrap();
        Poll::Ready(Ok(Reader {
            values: file.values.into(),
            gate: file.reading,
            drops: file.drops,
            _pin: PhantomPinned,
        }))
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
    type Open = ReadOpen;
    type Reader = Reader;
    fn frame_count(&self) -> u32 {
        self.count
    }
    fn open(&self) -> Self::Open {
        self.opens.set(self.opens.get() + 1);
        ReadOpen {
            file: Some(self.clone()),
            _pin: PhantomPinned,
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
pub fn frames(n: usize) -> (impl Stream<Item = Frame> + Unpin, Rc<Cell<usize>>) {
    let drops = Rc::new(Cell::new(0));
    let values: Vec<_> = (0..n)
        .map(|value| Frame {
            value,
            drops: drops.clone(),
        })
        .collect();
    (stream::iter(values), drops)
}
#[derive(Clone)]
pub struct Write {
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
pin_project! { pub struct WriteOpen { file: Option<Write>, #[pin] _pin: PhantomPinned } }
impl Future for WriteOpen {
    type Output = Result<Writer, &'static str>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let file = this.file.as_ref().unwrap();
        if !file.opening.ready(cx) {
            return Poll::Pending;
        }
        if file.open_failures.get() > 0 {
            file.open_failures.set(file.open_failures.get() - 1);
            return Poll::Ready(Err("open"));
        }
        let file = this.file.take().unwrap();
        let attempt = file.attempts.borrow().len();
        file.attempts.borrow_mut().push(Vec::new());
        Poll::Ready(Ok(Writer {
            file,
            attempt,
            _pin: PhantomPinned,
        }))
    }
}
pin_project! { pub struct Writer { file: Write, attempt: usize, #[pin] _pin: PhantomPinned }
    impl PinnedDrop for Writer { fn drop(this: Pin<&mut Self>) { this.file.drops.set(this.file.drops.get() + 1); } }
}
impl FrameWriter<Frame> for Writer {
    type Error = &'static str;
    type Output = Vec<usize>;
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        frame: &Frame,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if !this.file.writing.ready(cx) {
            return Poll::Pending;
        }
        let mut attempts = this.file.attempts.borrow_mut();
        let frames = &mut attempts[*this.attempt];
        if !frames.is_empty() && this.file.write_failures.get() > 0 {
            this.file
                .write_failures
                .set(this.file.write_failures.get() - 1);
            return Poll::Ready(Err("write"));
        }
        frames.push(frame.value);
        Poll::Ready(Ok(()))
    }
    fn poll_finalize(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::Output, Self::Error>> {
        let this = self.project();
        if !this.file.finalizing.ready(cx) {
            return Poll::Pending;
        }
        if this.file.finalize_failures.get() > 0 {
            this.file
                .finalize_failures
                .set(this.file.finalize_failures.get() - 1);
            return Poll::Ready(Err("finalize"));
        }
        this.file.finalized.set(this.file.finalized.get() + 1);
        Poll::Ready(Ok(this.file.attempts.borrow()[*this.attempt].clone()))
    }
}
impl WriteFile<Frame> for Write {
    type Error = &'static str;
    type Output = Vec<usize>;
    type Open = WriteOpen;
    type Writer = Writer;
    fn frame_capacity(&self) -> u32 {
        self.capacity
    }
    fn open(&self) -> Self::Open {
        self.opens.set(self.opens.get() + 1);
        WriteOpen {
            file: Some(self.clone()),
            _pin: PhantomPinned,
        }
    }
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
