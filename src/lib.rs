#![doc = include_str!("../README.md")]

mod config;
mod error;
mod traits;

pub use config::{SchedulerConfig, Window};
pub use error::{ContractError, SchedulerError};
pub use traits::{FrameWriter, ReadFile, WriteFile};

mod budget;
mod ready;
mod read;
pub use budget::{FrameBudget, FramePermit};
pub use read::ReadScheduler;
const MAX_POLL_OPS: usize = 256;
