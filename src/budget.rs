use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll, Waker},
};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct Request {
    minimum: usize,
    desired: usize,
    granted: usize,
    waiting: bool,
    waker: Option<Waker>,
}

struct State {
    available: usize,
    waiters: VecDeque<Weak<Mutex<Request>>>,
}

impl State {
    // Reserve before waking: competing polls cannot steal a waiter's capacity.
    fn dispatch(&mut self) -> Vec<Waker> {
        let mut wakes = Vec::new();
        while let Some(weak) = self.waiters.front() {
            let Some(request) = weak.upgrade() else {
                self.waiters.pop_front();
                continue;
            };
            let mut request = lock(&request);
            if !request.waiting {
                self.waiters.pop_front();
                continue;
            }
            if self.available < request.minimum {
                break;
            }
            let grant = if self.available >= request.desired {
                request.desired
            } else {
                request.minimum
            };
            self.available -= grant;
            request.granted = grant;
            request.waiting = false;
            self.waiters.pop_front();
            if let Some(waker) = request.waker.take() {
                wakes.push(waker);
            }
        }
        wakes
    }
}

/// Cloneable global capacity, measured in frames rather than bytes.
///
/// # Examples
///
/// ```
/// use carbon_io::FrameBudget;
///
/// let budget = FrameBudget::new(64);
/// assert_eq!(budget.total_capacity(), 64);
/// assert_eq!(budget.available_capacity(), 64);
/// ```
#[derive(Clone)]
pub struct FrameBudget {
    total: usize,
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for FrameBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameBudget")
            .field("total", &self.total)
            .field("available", &self.available_capacity())
            .finish()
    }
}

impl FrameBudget {
    /// Create a shared budget. Schedulers report `ZeroBudget` for zero capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use carbon_io::FrameBudget;
    ///
    /// let budget = FrameBudget::new(128);
    /// assert_eq!(budget.total_capacity(), 128);
    /// ```
    pub fn new(total_capacity: usize) -> Self {
        Self {
            total: total_capacity,
            state: Arc::new(Mutex::new(State {
                available: total_capacity,
                waiters: VecDeque::new(),
            })),
        }
    }
    /// Fixed total capacity, including grants currently reserved for waiters.
    pub fn total_capacity(&self) -> usize {
        self.total
    }
    /// Unreserved capacity. Intended for diagnostics, not the frame hot path.
    pub fn available_capacity(&self) -> usize {
        lock(&self.state).available
    }
    /// Create an empty local permit.
    pub fn permit(&self) -> FramePermit {
        FramePermit {
            budget: self.clone(),
            capacity: 0,
            request: Arc::new(Mutex::new(Request::default())),
        }
    }
}

/// A scheduler-local capacity grant. Drop returns all capacity and cancels waits.
///
/// # Examples
///
/// ```
/// use carbon_io::FrameBudget;
///
/// let budget = FrameBudget::new(64);
/// let permit = budget.permit();
/// assert_eq!(permit.capacity(), 0);
/// assert_eq!(permit.total_capacity(), 64);
/// ```
pub struct FramePermit {
    budget: FrameBudget,
    capacity: usize,
    request: Arc<Mutex<Request>>,
}

impl fmt::Debug for FramePermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FramePermit")
            .field("capacity", &self.capacity)
            .field("total_capacity", &self.total_capacity())
            .finish()
    }
}

impl FramePermit {
    /// Current local grant. Reading this does not access shared state.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    /// Total shared capacity.
    pub fn total_capacity(&self) -> usize {
        self.budget.total
    }
    /// Grow to at least `minimum`, preferably `desired`, both absolute sizes.
    ///
    /// Pending requests are FIFO and reserve capacity before waking. Call again
    /// with the same sizes until completion, or call `shrink_to` to cancel.
    ///
    /// # Panics
    /// Panics unless `minimum <= desired <= total_capacity()`.
    pub fn poll_grow(&mut self, cx: &mut Context<'_>, minimum: usize, desired: usize) -> Poll<()> {
        assert!(minimum <= desired && desired <= self.budget.total);
        if self.capacity >= minimum {
            return Poll::Ready(());
        }
        let mut state = lock(&self.budget.state);
        let mut request = lock(&self.request);
        self.capacity += std::mem::take(&mut request.granted);
        if self.capacity >= minimum {
            return Poll::Ready(());
        }
        if request.waiting {
            if request
                .waker
                .as_ref()
                .is_none_or(|old| !old.will_wake(cx.waker()))
            {
                request.waker = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let need = minimum - self.capacity;
        let want = desired - self.capacity;
        if state.waiters.is_empty() && state.available >= need {
            let grant = if state.available >= want { want } else { need };
            self.capacity += grant;
            state.available -= grant;
            return Poll::Ready(());
        }
        request.minimum = need;
        request.desired = want;
        request.waiting = true;
        request.waker = Some(cx.waker().clone());
        state.waiters.push_back(Arc::downgrade(&self.request));
        Poll::Pending
    }
    /// Return capacity above `capacity` and cancel any outstanding growth request.
    pub fn shrink_to(&mut self, capacity: usize) {
        let wakes = {
            let mut state = lock(&self.budget.state);
            let mut request = lock(&self.request);
            request.waiting = false;
            request.waker = None;
            state.available += std::mem::take(&mut request.granted);
            let keep = self.capacity.min(capacity);
            state.available += self.capacity - keep;
            self.capacity = keep;
            // Remove canceled entries before this permit can enqueue again.
            state
                .waiters
                .retain(|entry| !entry.ptr_eq(&Arc::downgrade(&self.request)));
            drop(request);
            state.dispatch()
        };
        for waker in wakes {
            waker.wake();
        }
    }
}

impl Drop for FramePermit {
    fn drop(&mut self) {
        self.shrink_to(0);
    }
}
