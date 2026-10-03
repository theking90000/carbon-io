use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
};

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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
            lock(&shared.queue).push_back(self.id);
            let parent = lock(&shared.parent).clone();
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
        let mut registered = lock(&self.shared.parent);
        if registered.as_ref().is_none_or(|old| !old.will_wake(parent)) {
            *registered = Some(parent.clone());
        }
        drop(registered);
        std::mem::swap(&mut *lock(&self.shared.queue), &mut self.notifications);
        while let Some(id) = self.notifications.pop_front() {
            self.signals[id].queued.store(false, Ordering::Release);
            self.schedule(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poisoned_notification_mutexes_still_deliver_work() {
        let mut queue = ReadyQueue::new();
        let id = queue.add();
        let shared = queue.shared.clone();
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = lock(&shared.queue);
                panic!("poison the notification queue");
            })
            .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = lock(&shared.parent);
                panic!("poison the parent registration");
            })
            .is_err()
        );
        let waker = futures::task::noop_waker();
        queue.register(&waker);
        queue.waker(id).wake_by_ref();
        queue.register(&waker);
        assert_eq!(queue.pop(), Some(id));
        assert_eq!(queue.pop(), None);
    }
}
