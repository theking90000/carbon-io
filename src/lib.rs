#![doc = include_str!("../README.md")]

mod budget;
mod config;
mod error;
mod read;
mod ready;
mod traits;
mod write;

pub use budget::{FrameBudget, FramePermit};
pub use config::{SchedulerConfig, Window};
pub use error::{ContractError, SchedulerError};
pub use read::{ReadEvent, ReadScheduler};
pub use traits::{ReadFile, WriteFile};
pub use write::{
    AsyncFilesWrite, BytesFrameAllocator, BytesPartialFrame, FilesWriteError, FrameAllocator,
    PartialFrame, WriteError, WriteScheduler, WriteStatus,
};

/// A pending event, an available event, EOF or a scheduler error.
pub type EventPoll<T, E> = std::task::Poll<Result<Option<T>, SchedulerError<E>>>;

/// Events requested from a scheduler, combined with `|`.
/// Unrequested admissions are neither returned nor newly reserved. Existing
/// reservations survive interest changes. Errors and EOF are always reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Interest(u8);

impl Interest {
    /// Drive I/O without returning admissions or outputs.
    pub const NONE: Self = Self(0);
    /// File admission for either scheduler.
    pub const FILES: Self = Self(1);
    /// Reserved for frame admission; ignored by `ReadScheduler`.
    pub const FRAMES: Self = Self(2);
    /// Ordered read frames or write finalization results.
    pub const RESULTS: Self = Self(4);
    /// All applicable events.
    pub const ALL: Self = Self(7);

    /// Whether all events in `other` are requested.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for Interest {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

const MAX_POLL_OPS: usize = 256;
