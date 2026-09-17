use crate::ContractError;

/// Independently controls buffering and anticipatory file opening.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    /// Desired local frame capacity. Writes with retries may require more.
    pub target_frames: usize,
    /// Distance from the logical front at which files may be opened.
    /// The effective discovery horizon is at least `target_frames`.
    pub open_ahead_frames: usize,
    /// Maximum simultaneous open operations and live readers or writers.
    pub max_active_files: usize,
}

impl Default for Window {
    fn default() -> Self {
        Self {
            target_frames: 256,
            open_ahead_frames: 1024,
            max_active_files: 16,
        }
    }
}

impl Window {
    pub(crate) fn validate(self) -> Result<(), ContractError> {
        if self.target_frames == 0 || self.max_active_files == 0 {
            Err(ContractError::InvalidWindow)
        } else {
            Ok(())
        }
    }
    pub(crate) fn horizon(self) -> usize {
        self.target_frames.max(self.open_ahead_frames)
    }
}

/// Fixed retry policy and dynamically replaceable window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SchedulerConfig {
    /// Buffer and opening horizons.
    pub window: Window,
    /// Number of retries after the initial attempt, shared across its stages.
    /// Zero releases write frames as soon as their individual write succeeds.
    pub max_retries: u32,
}
