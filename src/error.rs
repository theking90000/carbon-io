use std::{error::Error, fmt};

/// Violations of the scheduler's structural contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractError {
    /// A read file declares no frames.
    ZeroFrameCount,
    /// A write file has no capacity.
    ZeroFrameCapacity,
    /// A reader ended before its declared count.
    UnexpectedEof,
    /// A reader yielded a frame after its declared count.
    TooManyFrames,
    /// Input frames outlive the destination stream.
    MissingWriteFile,
    /// A replayable file cannot fit in the entire shared budget.
    FrameCapacityExceedsBudget,
    /// The global budget has zero capacity.
    ZeroBudget,
    /// The target or active-file limit is zero.
    InvalidWindow,
}

impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroFrameCount => "read frame count must be nonzero",
            Self::ZeroFrameCapacity => "write frame capacity must be nonzero",
            Self::UnexpectedEof => "reader ended before its declared frame count",
            Self::TooManyFrames => "reader exceeded its declared frame count",
            Self::MissingWriteFile => "no destination for the next input frame",
            Self::FrameCapacityExceedsBudget => "replayable file exceeds the global frame budget",
            Self::ZeroBudget => "frame budget must be nonzero",
            Self::InvalidWindow => "target frames and active file limit must be nonzero",
        })
    }
}
impl Error for ContractError {}

/// A terminal backend failure or contract violation. Emitted once, then EOF.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchedulerError<E> {
    /// Original backend error, without type erasure.
    Backend(E),
    /// Invalid configuration or file behavior.
    Contract(ContractError),
}
impl<E: fmt::Display> fmt::Display for SchedulerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(e) => write!(f, "backend: {e}"),
            Self::Contract(e) => e.fmt(f),
        }
    }
}
impl<E: Error + 'static> Error for SchedulerError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Backend(e) => e,
            Self::Contract(e) => e,
        })
    }
}
impl<E> From<ContractError> for SchedulerError<E> {
    fn from(error: ContractError) -> Self {
        Self::Contract(error)
    }
}
