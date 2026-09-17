use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
};

struct Shared {
    queue: Mutex<VecDeque<usize>>,
    parent: Mutex<Option<Waker>>,
}

struct Signal {
    id: usize,
    queued: AtomicBool,
    shared: Weak<Shared>,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        if !self.queued.swap(true, Ordering::AcqRel) {
            shared.queue.lock().unwrap().push_back(self.id);
            let parent = shared.parent.lock().unwrap().clone();
            if let Some(parent) = parent {
                parent.wake();
            }
        }
    }
}

/// Local ready work is lock-free; only external wake notifications synchronize.
pub(crate) struct ReadyQueue {
    shared: Arc<Shared>,
    signals: Vec<Arc<Signal>>,
    local: VecDeque<usize>,
    queued: Vec<bool>,
    notifications: VecDeque<usize>,
}

impl ReadyQueue {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(VecDeque::new()),
                parent: Mutex::new(None),
            }),
            signals: Vec::new(),
            local: VecDeque::new(),
            queued: Vec::new(),
            notifications: VecDeque::new(),
        }
    }
    pub(crate) fn add(&mut self) -> usize {
        let id = self.signals.len();
        self.signals.push(Arc::new(Signal {
            id,
            queued: AtomicBool::new(false),
            shared: Arc::downgrade(&self.shared),
        }));
        self.queued.push(false);
        id
    }
    pub(crate) fn schedule(&mut self, id: usize) {
        if !self.queued[id] {
            self.queued[id] = true;
            self.local.push_back(id);
        }
    }
    pub(crate) fn pop(&mut self) -> Option<usize> {
        let id = self.local.pop_front()?;
        self.queued[id] = false;
        Some(id)
    }
    pub(crate) fn has_work(&self) -> bool {
        !self.local.is_empty()
    }
    pub(crate) fn waker(&self, id: usize) -> Waker {
        Waker::from(self.signals[id].clone())
    }
    pub(crate) fn register(&mut self, parent: &Waker) {
        let mut registered = self.shared.parent.lock().unwrap();
        if registered.as_ref().is_none_or(|old| !old.will_wake(parent)) {
            *registered = Some(parent.clone());
        }
        drop(registered);
        std::mem::swap(
            &mut *self.shared.queue.lock().unwrap(),
            &mut self.notifications,
        );
        while let Some(id) = self.notifications.pop_front() {
            self.signals[id].queued.store(false, Ordering::Release);
            self.schedule(id);
        }
    }
}
