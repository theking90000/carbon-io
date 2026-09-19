use crate::ContractError;

/// Independently controls buffering and file discovery.
///
/// # Examples
///
/// ```
/// use carbon_io::Window;
///
/// let window = Window::default();
/// assert_eq!(window.target_frames, 256);
/// assert_eq!(window.open_ahead_frames, 1024);
/// assert_eq!(window.max_active_files, 16);
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Window {
    /// Desired local frame capacity. Writes with retries may require more.
    pub target_frames: usize,
    /// Distance from the logical front at which files may be discovered.
    /// The effective discovery horizon is at least `target_frames`.
    /// Reads may open ahead; writes open only after receiving their first frame.
    pub open_ahead_frames: usize,
    /// Maximum active files, including unopened write destinations.
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
    /// Create a validated window configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ContractError::InvalidWindow`] if `target_frames` or `max_active_files` is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use carbon_io::Window;
    ///
    /// let window = Window::new(128, 512, 8).unwrap();
    /// assert_eq!(window.target_frames, 128);
    /// ```
    pub fn new(
        target_frames: usize,
        open_ahead_frames: usize,
        max_active_files: usize,
    ) -> Result<Self, ContractError> {
        let window = Self {
            target_frames,
            open_ahead_frames,
            max_active_files,
        };
        window.validate()?;
        Ok(window)
    }

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

    /// Set the target frames.
    pub const fn with_target_frames(mut self, target_frames: usize) -> Self {
        self.target_frames = target_frames;
        self
    }

    /// Set the open ahead frames.
    pub const fn with_open_ahead_frames(mut self, open_ahead_frames: usize) -> Self {
        self.open_ahead_frames = open_ahead_frames;
        self
    }

    /// Set the maximum active files.
    pub const fn with_max_active_files(mut self, max_active_files: usize) -> Self {
        self.max_active_files = max_active_files;
        self
    }
}

/// Fixed retry policy and dynamically replaceable window.
///
/// # Examples
///
/// ```
/// use carbon_io::{SchedulerConfig, Window};
///
/// let config = SchedulerConfig::new(Window::default(), 2);
/// assert_eq!(config.max_retries, 2);
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SchedulerConfig {
    /// Buffer and discovery horizons.
    pub window: Window,
    /// Number of retries after the initial attempt, shared across its stages.
    /// Zero releases write frames as soon as their individual write succeeds.
    pub max_retries: u32,
}

impl SchedulerConfig {
    /// Create a new scheduler configuration with the given window and retry limit.
    ///
    /// # Examples
    ///
    /// ```
    /// use carbon_io::{SchedulerConfig, Window};
    ///
    /// let config = SchedulerConfig::new(Window::default(), 3);
    /// assert_eq!(config.max_retries, 3);
    /// ```
    pub const fn new(window: Window, max_retries: u32) -> Self {
        Self {
            window,
            max_retries,
        }
    }

    /// Set the window configuration.
    pub const fn with_window(mut self, window: Window) -> Self {
        self.window = window;
        self
    }

    /// Set the maximum retries.
    pub const fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}
