#![doc = include_str!("../README.md")]

mod budget;
mod config;
mod error;
mod read;
mod traits;
mod write;

pub use budget::{FrameBudget, FramePermit};
pub use config::{SchedulerConfig, Window};
pub use error::{ContractError, SchedulerError};
pub use read::{PushError, ReadScheduler};
pub use traits::{ReadFile, WriteFile};
pub use write::{
    AsyncFilesWrite, BytesFrameAllocator, BytesPartialFrame, FileCloseResult, FilesWriteError,
    FrameAllocator, PartialFrame, WriteError, WriteScheduler, WriteStatus,
};

const MAX_POLL_OPS: usize = 256;
