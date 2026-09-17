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
pub use read::ReadScheduler;
pub use traits::{FrameWriter, ReadFile, WriteFile};
pub use write::WriteScheduler;

const MAX_POLL_OPS: usize = 256;
